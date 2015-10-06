// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
'use strict';

var DampReqRequest = require('../../request_response.js').DampReqRequest;
var EventEmitter = require('events').EventEmitter;
var events = require('./events.js');
var globalTimers = require('timers');
var TypedError = require('error/typed');
var util = require('util');

var UnattainableRValError = TypedError({
    type: 'ringpop.damping.unattainable-rval',
    message: 'Unable to attain damp-req r-val',
    rVal: null,
    errors: null
});

function Damper(opts) {
    this.ringpop = opts.ringpop;
    this.timers = opts.timers || globalTimers;
    this.flappers = {}; // indexed by address (id)

    this.dampTimer = null;
    this.isDampTimerEnabled = false;

    // This is a temporary collection of damped members
    // since we're not actually damping them (marking the
    // member's status to damped) at this point.
    this.dampedMembers = {};
    this.dampedMemberExpirationTimer = null;
}

util.inherits(Damper, EventEmitter);

// This function definition is pulled up and attached to the constructor
// so that its output can be easily unit tested.
Damper.getMembersToDamp = function getMembersToDamp(allResponses, rVal, config) {
    var member;
    var scores;

    // res is an Array of flapper damp scores from each damp-req member.
    // Aggregate results by grouping damp scores by flapper.
    var dampScoresByFlapper = {};
    for (var i = 0; i < allResponses.length; i++) {
        var dampReqResponse = allResponses[i];
        var responseScores = dampReqResponse.scores;
        if (!Array.isArray(responseScores)) continue;
        for (var j = 0; j < responseScores.length; j++) {
            var responseScore = responseScores[j];
            member = responseScore.member;
            scores = dampScoresByFlapper[member] || [];
            scores.push(responseScore);
            dampScoresByFlapper[member] = scores;
        }
    }

    var membersToDamp = [];
    var keys = Object.keys(dampScoresByFlapper);
    for (i = 0; i < keys.length; i++) {
        member = keys[i];
        scores = dampScoresByFlapper[member];
        if (scores.length >= rVal && scoresExceedSuppressLimit(scores)) {
            membersToDamp.push(member);
        }
    }

    return membersToDamp;

    function scoresExceedSuppressLimit(scores) {
        var suppressLimit = config.get('dampScoringSuppressLimit');
        return scores.every(function each(score) {
            return score.dampScore >= suppressLimit;
        });
    }
};

Damper.prototype.addFlapper = function addFlapper(flapper) {
    if (this.dampedMembers[flapper.address]) {
        this.ringpop.logger.debug('ringpop member is already damped', {
            local: this.ringpop.whoami()
        });
        return;
    }

    if (this.flappers[flapper.address]) {
        this.ringpop.logger.debug('ringpop flapper already added to damper', {
            local: this.ringpop.whoami()
        });
        return;
    }

    this.flappers[flapper.address] = flapper;

    if (this._getFlapperCount() === 1) {
        this._startDampTimer();
    }
};

Damper.prototype.removeFlapper = function removeFlapper(flapper) {
    var address = flapper.address || flapper;
    if (!this.flappers[address]) {
        this.ringpop.logger.debug('ringpop flapper has not been added to the damper');
        return;
    }

    delete this.flappers[address];

    if (this._getFlapperCount() === 0) {
        this._stopDampTimer();
    }
};

Damper.prototype.initiateSubprotocol = function initiateSubprotocol(callback) {
    var self = this;
    var config = this.ringpop.config;
    var logger = this.ringpop.logger;

    // TODO Stop damp timer when cluster has reached damped limits
    if (!this._validateDampedClusterLimits()) {
        return;
    }

    var flapperAddrs = this._getFlapperAddrs();
    logger.info('ringpop starting damping subprotocol', {
        local: this.ringpop.whoami(),
        flappers: flapperAddrs
    });

    // Select members to receive the damp-req.
    var nVal = config.get('dampReqNVal');
    var dampReqMembers = this.ringpop.membership.getRandomPingableMembers(
        nVal, flapperAddrs);
    var dampReqMemberAddrs = dampReqMembers.map(function map(member) {
        return member.address;
    });

    // Make sure rVal is satisfiable to begin with.
    var rVal = Math.min(config.get('dampReqRVal'), dampReqMembers.length);
    if (rVal === 0) {
        logger.warn('ringpop damping aborted due to lack of selectable damp req members', {
            local: this.ringpop.whoami(),
            flappers: flapperAddrs,
            rVal: rVal,
            nVal: nVal,
            numDampReqMembers: dampReqMembers.length
        });
        this.emit('event', new events.DampReqUnsatisfiedEvent());
        return;
    }

    this._fanoutDampReqs(flapperAddrs, dampReqMembers, rVal, onDampReqs);

    function onDampReqs(err, res) {
        if (err) {
            logger.warn('ringpop damping subprotocol errored out', {
                local: self.ringpop.whoami(),
                dampReqMembers: dampReqMemberAddrs,
                errors: err
            });
            self.emit('event', new events.DampReqFailedEvent());
            callback();
            return;
        }

        var membersToDamp = Damper.getMembersToDamp(res, rVal, config);
        if (membersToDamp.length === 0) {
            self.ringpop.logger.warn('ringpop damping subprotocol inconclusive', {
                local: self.ringpop.whoami(),
                dampReqMembers: dampReqMemberAddrs,
                results: res
            });
            return;
        }

        for (var i = 0; i < membersToDamp.length; i++) {
            self._dampMember(membersToDamp[i]);
        }

        self.ringpop.logger.warn('ringpop damped members', {
            local: self.ringpop.whoami(),
            dampReqMembers: dampReqMemberAddrs,
            membersToDamp: membersToDamp,
            results: res
        });
        self.emit('event', new events.DampedEvent());
        callback();
    }
};

Damper.prototype._dampMember = function _dampMember(memberAddr)  {
    var self = this;
    this.ringpop.membership.makeDamped(memberAddr);

    // Don't start anymore damping subprotocols for a member
    // that is already damped.
    this.removeFlapper(memberAddr);
    this.dampedMembers[memberAddr] = {
        timestamp: Date.now()
    };

    if (!this.dampedMemberExpirationTimer) {
        scheduleExpiration();
        this.ringpop.logger.info('ringpop started damped member expiration timer', {
            local: this.ringpop.whoami()
        });
    }

    function scheduleExpiration() {
        self.dampedMemberExpirationTimer =
            self.timers.setTimeout(function onTimeout() {
                self._expireDampedMembers();
                scheduleExpiration();
        }, self.ringpop.config.get('dampedMemberExpirationInterval'));
    }
};

Damper.prototype._expireDampedMembers = function _expireDampedMembers() {
    var undampedMembers = [];
    var suppressDuration = this.ringpop.config.get('dampScoringSuppressDuration');
    var keys = Object.keys(this.dampedMembers);
    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        var memberDamping = this.dampedMembers[keys[i]];
        var dampedDuration = Date.now() - memberDamping.timestamp;
        if (dampedDuration >= suppressDuration) {
            delete this.dampedMembers[key];
            undampedMembers.push({
                member: key,
                timeDamped: dampedDuration
            });
        }
    }

    if (undampedMembers.length > 0) {
        this.ringpop.logger.info('ringpop has undamped members', {
            local: this.ringpop.whoami(),
            suppressDuration: suppressDuration,
            undampedMembers: undampedMembers
        });
    }

    // If there are no more damped members, stop trying to expire them.
    if (Object.keys(this.dampedMembers) === 0) {
        this.timers.clearTimeout(this.dampedMemberExpirationTimer);
        this.dampedMemberExpirationTimer = null;
        this.ringpop.logger.info('ringpop stopped damped member expiration timer', {
            local: this.ringpop.whoami()
        });
    }
};

Damper.prototype._fanoutDampReqs = function _fanoutDampReqs(flapperAddrs, dampReqMembers, rVal, callback) {
    var self = this;

    // Send out the damp-req to each member selected.
    var request = new DampReqRequest(this.ringpop, flapperAddrs);
    for (var i = 0; i < dampReqMembers.length; i++) {
        var dampReqAddr = dampReqMembers[i].address;
        this.ringpop.stat('increment', 'damp-req.send');
        this.ringpop.client.protocolDampReq(dampReqAddr, request,
            dampReqCallback(dampReqAddr));
    }

    var numPendingReqs = dampReqMembers.length;
    var errors = [];
    var results = [];

    // Accumulate responses until rVal is satisfied or is impossible to satisfy because
    // too many error responses.
    function dampReqCallback(addr) {
        return function onDampReq(err, res) {
            // Prevents double-callback
            if (typeof callback !== 'function') return;

            numPendingReqs--;

            if (err) {
                errors.push(err);
            } else {
                if (Array.isArray(res.changes)) {
                    self.ringpop.membership.update(res.changes);
                }

                // Enrich the result with the addr of the damp
                // req member for reporting purposes.
                res.dampReqAddr = addr;
                results.push(res);
            }

            // The first rVal requests will be reported.
            if (results.length >= rVal) {
                callback(null, results);
                callback = null;
                return;
            }

            if (numPendingReqs < rVal - results.length) {
                callback(UnattainableRValError({
                    flappers: flapperAddrs,
                    rVal: rVal,
                    errors: errors
                }));
                callback = null;
                return;
            }
        };
    }
};

Damper.prototype._getFlapperAddrs = function _getFlapperAddrs() {
    return Object.keys(this.flappers);
};

Damper.prototype._getFlapperCount = function _getFlapperCount() {
    return Object.keys(this.flappers).length;
};

Damper.prototype._startDampTimer = function _startDampTimer() {
    var self = this;

    if (this.isDampTimerEnabled) {
        this.ringpop.logger.info('ringpop damp timer already started', {
            local: this.ringpop.whoami()
        });
        return;
    }

    this.isDampTimerEnabled = true;
    schedule();
    this.ringpop.logger.debug('ringpop damp timer started', {
        local: this.ringpop.whoami()
    });

    function schedule() {
        if (!self.isDampTimerEnabled) {
            return;
        }

        self.dampTimer = self.timers.setTimeout(function onTimeout() {
            self.initiateSubprotocol(function onSubprotocol() {
                schedule();
            });
        }, self.ringpop.config.get('dampTimerInterval'));
    }
};

Damper.prototype._stopDampTimer = function _stopDampTimer() {
    if (!this.isDampTimerEnabled) {
        this.ringpop.logger.debug('ringpop damp timer already stopped', {
            local: this.ringpop.whoami()
        });
        return;
    }

    this.timers.clearTimeout(this.dampTimer);
    this.dampTimer = null;
    this.isDampTimerEnabled = false;
    this.ringpop.logger.debug('ringpop damp timer stopped', {
        local: this.ringpop.whoami()
    });
};

Damper.prototype._validateDampedClusterLimits = function _validateDampedClusterLimits() {
    // Determine if the portion of damped members in the cluster exceeds
    // the maximum limit.
    var dampedCurrent = this.ringpop.membership.getDampedPercentage();
    var dampedMax = this.ringpop.config.get('dampedMaxPercentage');
    if (dampedCurrent < dampedMax) {
        return true;
    }

    this.ringpop.logger.warn('ringpop damping reached maximum allowable damped members', {
        local: this.ringpop.whoami(),
        dampedCurrent: dampedCurrent,
        dampedMax: dampedMax
    });
    this.emit('event', new events.DampedLimitExceededEvent());
    return false;
};

module.exports = Damper;
