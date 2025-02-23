# -*- coding: utf-8 -*-
import time
import unittest

from plumpy.base import state_machine

# Events
PLAY = 'Play'
PAUSE = 'Pause'
STOP = 'Stop'

# States
PLAYING = 'Playing'
PAUSED = 'Paused'
STOPPED = 'Stopped'


class Playing(state_machine.State):
    LABEL = PLAYING
    ALLOWED = {PAUSED, STOPPED}
    TRANSITIONS = {STOP: STOPPED}

    def __init__(self, player, track):
        assert track is not None, 'Must provide a track name'
        super().__init__(player)
        self.track = track
        self._last_time = None
        self._played = 0.0

    def __str__(self):
        if self.in_state:
            self._update_time()
        return f'> {self.track} ({self._played}s)'

    def enter(self):
        super().enter()
        self._last_time = time.time()

    def exit(self):
        super().exit()
        self._update_time()

    def play(self, track=None):
        return False

    def _update_time(self):
        current_time = time.time()
        self._played += current_time - self._last_time
        self._last_time = current_time


class Paused(state_machine.State):
    LABEL = PAUSED
    ALLOWED = {PLAYING, STOPPED}
    TRANSITIONS = {STOP: STOPPED}

    def __init__(self, player, playing_state):
        assert isinstance(playing_state, Playing), 'Must provide the playing state to pause'
        super().__init__(player)
        self._player = player
        self.playing_state = playing_state

    def __str__(self):
        return f'|| ({self.playing_state})'

    def play(self, track=None):
        if track is not None:
            self.state_machine.transition_to(Playing(player=self.state_machine, track=track))
        else:
            self.state_machine.transition_to(self.playing_state)


class Stopped(state_machine.State):
    LABEL = STOPPED
    ALLOWED = {
        PLAYING,
    }
    TRANSITIONS = {PLAY: PLAYING}

    def __str__(self):
        return '[]'

    def play(self, track):
        self.state_machine.transition_to(Playing(self.state_machine, track=track))


class CdPlayer(state_machine.StateMachine):
    STATES = (Stopped, Playing, Paused)

    def __init__(self):
        super().__init__()
        self.add_state_event_callback(
            state_machine.StateEventHook.ENTERING_STATE, lambda _s, _h, state: self.entering(state)
        )
        self.add_state_event_callback(state_machine.StateEventHook.EXITING_STATE, lambda _s, _h, _st: self.exiting())

    def entering(self, state):
        print(f'Entering {state}')
        print(self._state)

    def exiting(self):
        print(f'Exiting {self.state}')
        print(self._state)

    @state_machine.event(to_states=Playing)
    def play(self, track=None):
        return self._state.play(track)

    @state_machine.event(from_states=Playing, to_states=Paused)
    def pause(self):
        self.transition_to(Paused(self, playing_state=self._state))
        return True

    @state_machine.event(from_states=(Playing, Paused), to_states=Stopped)
    def stop(self):
        self.transition_to(Stopped(self))


class TestStateMachine(unittest.TestCase):
    def test_basic(self):
        cd_player = CdPlayer()
        self.assertEqual(cd_player.state, STOPPED)

        cd_player.play('Eminem - The Real Slim Shady')
        self.assertEqual(cd_player.state, PLAYING)
        time.sleep(1.0)

        cd_player.pause()
        self.assertEqual(cd_player.state, PAUSED)

        cd_player.play()
        self.assertEqual(cd_player.state, PLAYING)

        self.assertEqual(cd_player.play(), False)

        cd_player.stop()
        self.assertEqual(cd_player.state, STOPPED)

    def test_invalid_event(self):
        cd_player = CdPlayer()
        with self.assertRaises(AssertionError):
            cd_player.play()
