from .utils import call_with_super_check, super_check

class State(object):
    LABEL = None

    # A set containing the labels of states that can be entered
    # from this one
    ALLOWED = set()

    @classmethod
    def is_terminal(cls) -> bool:
        return not cls.ALLOWED

    def __init__(self, state_machine):
        """
        :param state_machine: The process this state belongs to
        :type state_machine: :class:`StateMachine`
        """
        self.state_machine = state_machine
        self.in_state = False

    def __str__(self):
        return str(self.LABEL)

    @property
    def label(self):
        return self.LABEL

    @super_check
    def enter(self):
        """ Entering the state"""
        pass

    def execute(self):
        """
        Execute the state, performing the actions that this State is responsible
        for. Return a state to transition to or None if finished
        """
        pass

    def exit(self):
        """ Exiting the state"""
        if self.is_terminal():
            raise InvalidStateError("Cannot exit a terminal state {}".format(self.LABEL))
        pass

    def create_state(self, state_label, *args, **kwargs):
        return self.state_machine.create_state(state_label, *args, **kwargs)

    def do_enter(self):
        call_with_super_check(self.enter)
        self.in_state = True

    def do_exit(self):
        call_with_super_check(self.exit)
        self.in_state = False
