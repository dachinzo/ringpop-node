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
var fixtures = require('../fixtures.js');
var DampReqResponse = require('../../request_response.js').DampReqResponse;
var testRingpop = require('../lib/test-ringpop.js');

function setupMembership(deps, numMembers) {
    numMembers = numMembers || 3;

    var membership = deps.membership;
    var memberGen = fixtures.memberGenerator(deps.ringpop);
    var members = [];
    for (var i = 0; i < numMembers; i++) {
        var member = memberGen();
        membership.makeAlive(member.address, member.incarnationNumber);
        members.push(member);
    }
    return members;
}

function stubClient(deps, protocolDampReq) {
    deps.ringpop.client = {
        destroy: function noop() {},
        protocolDampReq: protocolDampReq
    };
}

testRingpop('damped max percentage', function t(deps, assert) {
    assert.plan(1);

    var ringpop = deps.ringpop;
    var config = ringpop.config;
    var damping = deps.membership.damping;

    // Lower allowable damped members to 0%
    config.set('dampedMaxPercentage', 0);

    damping.on('event', function onEvent(event) {
        assert.equals(event.name, 'DampedLimitExceededEvent',
            'damped limit exceeded event');
    });
    damping.initiateSubprotocol(fixtures.member1(ringpop));
});

testRingpop('damping in progress', function t(deps, assert) {
    assert.plan(1);

    // Stub out client so that the first subprotocol run
    // is delayed behind the next. This'll cause a damping
    // in progress event to be emitted.
    stubClient();
    var members = setupMembership(deps);
    var damping = deps.membership.damping;
    damping.on('event', function onEvent(event) {
        if (event.name === 'DampingInProgressEvent') {
            assert.pass('damping in progress');
        }
    });
    damping.initiateSubprotocol(members[0]);
    damping.initiateSubprotocol(members[0]);

    function stubClient() {
        deps.ringpop.client = {
            destroy: function noop() {},
            protocolDampReq: function protocolDampReq(host, body, callback) {
                process.nextTick(function onTick() {
                    callback(null, []);
                });
            }
        };
    }
});

testRingpop('damp-req selection unsatisfied', function t(deps, assert) {
    assert.plan(1);

    var damping = deps.membership.damping;
    damping.on('event', function onEvent(event) {
        if (event.name === 'DampReqUnsatisfiedEvent') {
            assert.pass('damp req selection unsatisfied');
        }
    });
    damping.initiateSubprotocol(fixtures.member1(deps.ringpop));
});

testRingpop({
    async: true
}, 'sends n-val damp-reqs', function t(deps, assert, done) {
    assert.plan(2);

    var nVal = 10;
    deps.config.set('dampReqNVal', nVal);
    deps.config.set('dampReqRVal', nVal);

    // Create enough members to satisfy damp-req selection
    var targets = setupMembership(deps, nVal + 1);

    // Remove member from list when a damp-req is sent to member.
    stubClient(deps, function protocolDampReq(host, body, callback) {
        targets = targets.filter(function filter(member) {
            return member.address !== host;
        });

        process.nextTick(function onTick() {
            callback(null, new DampReqResponse(deps.ringpop, body, {
                dampScore: 0
            }));
        });
    });

    var damping = deps.membership.damping;
    var flappyMember = targets[targets.length - 1];
    // By the time unconfirmed event is emitted all members
    // should have received a damp-req.
    damping.on('event', function onEvent(event) {
        if (event.name === 'DampingUnconfirmedEvent') {
            assert.equals(targets.length, 1, 'all n received damp req');
            // Only one not to be filtered out: the flappy member itself.
            assert.equals(targets[0].address, flappyMember.address,
                'flappy member not filtered out');
            done();
        }
    });
    damping.initiateSubprotocol(flappyMember);
});
