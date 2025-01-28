# -*- coding: utf-8 -*-
from plumpy import ProcessSpec
from plumpy.ports import InputPort, PortNamespace
import unittest


class StrSubtype(str):
    pass


class TestProcessSpec(unittest.TestCase):
    def setUp(self):
        self.spec = ProcessSpec()

    def test_get_port_namespace_base(self):
        """
        Get the root, inputs and outputs port namespaces of the ProcessSpec
        """
        input_ports = self.spec.inputs
        output_ports = self.spec.outputs

        assert input_ports.name, self.spec.NAME_INPUTS_PORT_NAMESPACE
        assert output_ports.name, self.spec.NAME_OUTPUTS_PORT_NAMESPACE

    def test_dynamic_output(self):
        self.spec.outputs.dynamic = True
        self.spec.outputs.valid_type = str
        assert self.spec.outputs.validate({'dummy': 'foo'}) is None
        assert self.spec.outputs.validate({'dummy': StrSubtype('bar')}) is None
        assert self.spec.outputs.validate({'dummy': 5}) is not None

        # Remove dynamic output
        self.spec.outputs.dynamic = False
        self.spec.outputs.valid_type = None

        # Now add and check behaviour
        self.spec.outputs.dynamic = True
        self.spec.outputs.valid_type = str
        assert self.spec.outputs.validate({'dummy': 'foo'}) is None
        assert self.spec.outputs.validate({'dummy': StrSubtype('bar')}) is None
        assert self.spec.outputs.validate({'dummy': 5}) is not None

    def test_get_description(self):
        spec = ProcessSpec()

        # Adding an input should create some description
        spec.input('test')
        description = spec.get_description()
        assert description != {}

        # Similar with adding output
        spec = ProcessSpec()
        spec.output('test')
        description = spec.get_description()
        assert description != {}

    def test_input_namespaced(self):
        """
        Test the creation of a namespaced input port
        """
        self.spec.input('some.name.space.a', valid_type=int)

        assert 'some' in self.spec.inputs
        assert 'name' in self.spec.inputs['some']
        assert 'space' in self.spec.inputs['some']['name']
        assert 'a' in self.spec.inputs['some']['name']['space']

        assert isinstance(self.spec.inputs.get_port('some'), PortNamespace)
        assert isinstance(self.spec.inputs.get_port('some.name'), PortNamespace)
        assert isinstance(self.spec.inputs.get_port('some.name.space'), PortNamespace)
        assert isinstance(self.spec.inputs.get_port('some.name.space.a'), InputPort)

    def test_validator(self):
        """Test the port validator with default."""

        def dict_validator(dictionary, port):
            if 'key' not in dictionary or dictionary['key'] != 'value':
                return 'Invalid dictionary'

        self.spec.input('dict', default={'key': 'value'}, validator=dict_validator)

        processed = self.spec.inputs.pre_process({})
        assert processed == {'dict': {'key': 'value'}}
        self.spec.inputs.validate()

        processed = self.spec.inputs.pre_process({'dict': {'key': 'value'}})
        assert processed == {'dict': {'key': 'value'}}
        self.spec.inputs.validate()

        assert self.spec.inputs.validate({'dict': {'wrong_key': 'value'}}) is not None

    def test_validate(self):
        """Test the global spec validator functionality."""

        def is_valid(inputs, port):
            if not ('a' in inputs) ^ ('b' in inputs):
                return 'Must have a OR b in inputs'
            return

        self.spec.input('a', required=False)
        self.spec.input('b', required=False)
        self.spec.inputs.validator = is_valid

        processed = self.spec.inputs.pre_process({'a': 'a'})
        assert processed == {'a': 'a'}
        self.spec.inputs.validate()

        processed = self.spec.inputs.pre_process({'b': 'b'})
        assert processed == {'b': 'b'}
        self.spec.inputs.validate()

        assert self.spec.inputs.validate({}) is not None
        assert self.spec.inputs.validate({'a': 'a', 'b': 'b'}) is not None
