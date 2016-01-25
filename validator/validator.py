#!/usr/bin/env python
# coding: utf-8

import abc
import string
from collections import namedtuple

import os

import sys
import yaml

Item = namedtuple('Item', 'value, start_mark, end_mark')
Stop = namedtuple('Stop', 'key, name, direction, latitude, longitude')
Route = namedtuple('Route', 'number, description, hidden, stops, trips')
RouteStop = namedtuple('RouteStop', 'key, shift')
RouteTrip = namedtuple('RouteTrip', 'workdays, weekend, everyday')
Routes = namedtuple('Routes', 'routes')
Stops = namedtuple('Stops', 'stops')


CONTENT_RELATIVE_PATH = '../content'


def main():
    try:
        content = Content.from_directory(get_content_dir())
        validate_content(content)
    except ValidationError as e:
        print(e, file=sys.stderr)
        return -1

    print('Content is valid.')


def get_content_dir():
    return os.path.abspath(
        os.path.join(get_script_dir(), CONTENT_RELATIVE_PATH)
    )


def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def validate_content(content):
    validators = [
        StopKeyUniquenessValidator(),
        StopKeyReferentialIntegrityValidator()
    ]

    for validator in validators:
        validator.validate(content)


class Content:
    STOPS_SUBDIR = 'stops'
    ROUTES_SUBDIR = 'routes'

    def __init__(self, stops, routes):
        self.stops = stops
        self.routes = routes

    @classmethod
    def from_directory(cls, directory):
        stops = cls._read_stops(os.path.join(directory, cls.STOPS_SUBDIR))
        routes = cls._read_routes(os.path.join(directory, cls.ROUTES_SUBDIR))

        return Content(stops, routes)

    @classmethod
    def _read_stops(cls, directory):
        stops = []

        producer = StopsProducer()
        for root in cls._enumerate_yaml_roots(directory):
            stops += producer.produce(root).value.stops.value

        return stops

    @classmethod
    def _enumerate_yaml_roots(cls, directory):
        file_names = (os.path.join(directory, x) for x in os.listdir(directory)
                      if cls._is_yaml_file(os.path.join(directory, x)))
        for file_name in file_names:
            with open(file_name, encoding='utf8') as file:
                yield yaml.compose(file)

    @classmethod
    def _is_yaml_file(cls, file_name):
        return os.path.isfile(file_name) and file_name.endswith('.yaml')

    @classmethod
    def _read_routes(cls, directory):
        routes = []

        producer = RoutesProducer()
        for root in cls._enumerate_yaml_roots(directory):
            routes += producer.produce(root).value.routes.value

        return routes


class ContentValidator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def validate(self, content):
        pass


class StopKeyUniquenessValidator(ContentValidator):
    def validate(self, content):
        used_keys = {}
        for stop in (x.value for x in content.stops):
            key = stop.key.value
            if key in used_keys:
                raise KeySecondUsageError(key, stop.key, used_keys[key])
            used_keys[key] = stop.key


class StopKeyReferentialIntegrityValidator(ContentValidator):
    def validate(self, content):
        valid_stop_keys = set(x.value.key.value for x in content.stops)

        for route_stops in (x.value.stops.value for x in content.routes):
            for key_item in (y.value.key for y in route_stops):
                if key_item.value not in valid_stop_keys:
                    raise SimpleValidationError.from_item(
                        'Undeclared stop key "{}"'.format(key_item.value),
                        key_item
                    )


class ItemProducer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def produce(self, node):
        """
        Produce an `Item` from given `node`
        """
        pass


class ScalarProducer(ItemProducer):
    def __init__(self, extractor, *validators):
        self._extractor = extractor
        self._validators = validators

    def produce(self, node):
        if not isinstance(node, yaml.ScalarNode):
            raise SimpleValidationError.from_node('Scalar expected', node)

        value = self._extractor.extract(node)
        for validator in self._validators:
            validator.validate(value, node)

        return Item(
            value=value,
            start_mark=node.start_mark,
            end_mark=node.end_mark
        )


class ListProducer(ItemProducer):
    def __init__(self, list_item_producer, *validators):
        self._list_item_producer = list_item_producer
        self._validators = validators

    def produce(self, node):
        if not isinstance(node, yaml.SequenceNode):
            raise SimpleValidationError.from_node('Sequence expected', node)

        value = [self._list_item_producer.produce(x) for x in node.value]
        for validator in self._validators:
            validator.validate(value, node)

        return Item(
            value=value,
            start_mark=node.start_mark,
            end_mark=node.end_mark
        )


class NamedTupleProducer(ItemProducer):
    class ProducerDescriptor:
        def __init__(self, key, producer, required, produced):
            self.key = key
            self.producer = producer
            self.required = required
            self.produced = produced

    def __init__(self, tuple_class, required_attr_producers=None,
                 optional_attr_producers=None, validators=None):

        self._tuple_class = tuple_class
        self._producer_descriptors = self._make_descriptors(
            required_attr_producers or {},
            optional_attr_producers or {}
        )
        self._validators = validators or []
        self._key_producer = ScalarProducer(
            StringValueExtractor(), NonEmptyStringValidator()
        )

    def _make_descriptors(self, required_producers, optional_producers):
        descriptors = {}

        for key, producer in required_producers.items():
            descriptors[key] = self.ProducerDescriptor(
                key, producer, True, False
            )

        for key, producer in optional_producers.items():
            if key in descriptors:
                raise RuntimeError('Key {0} used more than once'.format(key))
            descriptors[key] = self.ProducerDescriptor(
                key, producer, False, False
            )

        return descriptors

    def produce(self, node):
        if not isinstance(node, yaml.MappingNode):
            raise SimpleValidationError.from_node('Mapping expected', node)

        tuple_dict = {x: None for x in self._tuple_class._fields}

        for key_node, value_node in node.value:
            descriptor = self._get_descriptor(key_node)
            value = descriptor.producer.produce(value_node)
            tuple_dict[descriptor.key] = value
            descriptor.produced = True

        self._validate_required_produced(node)
        self._clear_produced_flag()

        value = self._tuple_class(**tuple_dict)

        for validator in self._validators:
            validator.validate(value, node)

        return Item(
            value=value,
            start_mark=node.start_mark,
            end_mark=node.end_mark
        )

    def _get_descriptor(self, key_node):
        key = self._key_producer.produce(key_node).value
        if key not in self._producer_descriptors:
            raise SimpleValidationError.from_node(
                'Item "{}" not expected'.format(key), key_node
            )

        descriptor = self._producer_descriptors[key]
        if descriptor.produced:
            raise SimpleValidationError.from_node(
                'Item "{}" used again'.format(key), key_node
            )

        return descriptor

    def _validate_required_produced(self, node):
        non_produced = next(
            (x for x in self._producer_descriptors.values()
             if x.required and not x.produced),
            None
        )
        if non_produced:
            raise SimpleValidationError.from_node(
                'Required item "{0}" not specified'.format(non_produced.key),
                node
            )

    def _clear_produced_flag(self):
        for descriptor in self._producer_descriptors.values():
            descriptor.produced = False


class StopProducer(NamedTupleProducer):
    def __init__(self):
        super().__init__(
            tuple_class=Stop,
            required_attr_producers=dict(
                key=ScalarProducer(
                    StringValueExtractor(),
                    NonEmptyStringValidator(),
                    StringKeyValidator()
                ),
                name=ScalarProducer(
                    StringValueExtractor(), NonEmptyStringValidator()
                ),
                latitude=ScalarProducer(
                    FloatValueExtractor(), LatitudeFloatRangeValidator()
                ),
                longitude=ScalarProducer(
                    FloatValueExtractor(), LongitudeFloatRangeValidator()
                )
            ),
            optional_attr_producers=dict(
                direction=ScalarProducer(
                    StringValueExtractor(), NonEmptyStringValidator()
                )
            )
        )


class RouteStopProducer(NamedTupleProducer):
    def __init__(self):
        super().__init__(
            tuple_class=RouteStop,
            required_attr_producers=dict(
                key=ScalarProducer(
                    StringValueExtractor(),
                    NonEmptyStringValidator(), StringKeyValidator()
                ),
                shift=ScalarProducer(
                    StringValueExtractor(),
                    NonEmptyStringValidator(), StringTimeShiftValidator()
                )
            )
        )


class RouteTripProducer(NamedTupleProducer):
    def __init__(self):
        time_list_producer = ListProducer(
            ScalarProducer(
                StringValueExtractor(),
                NonEmptyStringValidator(), StringTimeShiftValidator()
            )
        )

        super().__init__(
            tuple_class=RouteTrip,
            optional_attr_producers=dict(
                workdays=time_list_producer,
                weekend=time_list_producer,
                everyday=time_list_producer
            ),
            validators=[RouteTripValidator()]
        )


class RouteProducer(NamedTupleProducer):
    def __init__(self):
        super().__init__(
            tuple_class=Route,
            required_attr_producers=dict(
                number=ScalarProducer(
                    StringValueExtractor(), NonEmptyStringValidator()
                ),
                description=ScalarProducer(
                    StringValueExtractor(), NonEmptyStringValidator()
                ),
                stops=ListProducer(RouteStopProducer()),
                trips=RouteTripProducer()
            ),
            optional_attr_producers=dict(
                hidden=ScalarProducer(BoolValueExtractor())
            )
        )


class StopsProducer(NamedTupleProducer):
    def __init__(self):
        super().__init__(
            tuple_class=Stops,
            required_attr_producers=dict(stops=ListProducer(StopProducer()))
        )


class RoutesProducer(NamedTupleProducer):
    def __init__(self):
        super().__init__(
            tuple_class=Routes,
            required_attr_producers=dict(routes=ListProducer(RouteProducer()))
        )


class ValidationError(Exception):
    def _print_mark(self, mark):
        return 'line {}, column {}'.format(mark.line + 1, mark.column + 1)


class SimpleValidationError(ValidationError):
    def __init__(self, message, start_mark, end_mark):
        self.message = message
        self.start_mark = start_mark
        self.end_mark = end_mark

    @classmethod
    def from_node(cls, message, node):
        return SimpleValidationError(message, node.start_mark, node.end_mark)

    @classmethod
    def from_item(cls, message, item):
        return SimpleValidationError(message, item.start_mark, item.end_mark)

    def __str__(self, *args, **kwargs):
        return '{}.\nFile: {}.\nStart: {}; end: {}.'.format(
            self.message,
            self.start_mark.name,
            self._print_mark(self.start_mark),
            self._print_mark(self.end_mark)
        )


class KeySecondUsageError(ValidationError):
    def __init__(self, key, item, first_use_item):
        self.key = key
        self.item = item
        self.first_use_item = first_use_item

    def __str__(self, *args, **kwargs):
        return ('Key "{}" used second time.\n\n'
                'At:\n{}\n\n'
                'First Usage:\n{}.').format(
            self.key,
            self._print_item(self.item),
            self._print_item(self.first_use_item)
        )

    def _print_item(self, item):
        return 'File: {}.\nStart: {}; end: {}'.format(
            item.start_mark.name,
            self._print_mark(item.start_mark),
            self._print_mark(item.end_mark)
        )


class ValueValidator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def validate(self, value, node):
        """
        Validate `value` previously extracted from `node`, raise `YamlError`
        if `value` is invalid
        """
        pass


class NonEmptyStringValidator(ValueValidator):
    def validate(self, value, node):
        if not value:
            raise SimpleValidationError.from_node(
                'Non empty value required', node
            )


class StringKeyValidator(ValueValidator):
    ALLOWED_CHARS = string.ascii_lowercase + string.digits + '-'

    def validate(self, value, node):
        invalid_char = next(
            (x for x in value if x not in self.ALLOWED_CHARS),
            None
        )

        if invalid_char:
            raise SimpleValidationError.from_node(
                'Invalid character "{}" in "{}"'.format(invalid_char, value),
                node
            )


class StringTimeShiftValidator(ValueValidator):
    def validate(self, value, node):
        if len(value) != len('hh:mm'):
            self._raise(value, node)

        self._to_positive_int(value[0:2], value, node)

        separator = value[2:3]
        if separator != ':':
            self._raise(value, node)

        minute = self._to_positive_int(value[3:5], value, node)
        if not 0 <= minute <= 59:
            self._raise(value, node)

    def _raise(self, value, node):
        raise SimpleValidationError.from_node(
            '"{}" is not a valid time'.format(node.value), node
        )

    def _to_positive_int(self, string_value, value, node):
        try:
            int_value = int(string_value)
        except ValueError:
            self._raise(value, node)

        if int_value < 0:
            self._raise(value, node)

        return int_value


class FloatRangeValidator(ValueValidator):
    def __init__(self, from_inclusive, to_inclusive):
        self._to_inclusive = to_inclusive
        self._from_inclusive = from_inclusive

    def validate(self, value, node):
        if not self._from_inclusive <= value <= self._to_inclusive:
            raise SimpleValidationError.from_node(
                'Value expected to be in {}..{} interval'.format(
                    self._from_inclusive, self._to_inclusive
                ),
                node
            )


class LatitudeFloatRangeValidator(FloatRangeValidator):
    # Novopolotsk and Polotsk neighbourhood
    FROM = 55.4
    TO = 55.6

    def __init__(self):
        super().__init__(self.FROM, self.TO)


class LongitudeFloatRangeValidator(FloatRangeValidator):
    # Novopolotsk and Polotsk neighbourhood
    FROM = 28.4
    TO = 28.9

    def __init__(self):
        super().__init__(self.FROM, self.TO)


class RouteTripValidator(ValueValidator):
    def validate(self, value, node):
        workdays_set = value.workdays is not None
        weekend_set = value.weekend is not None
        everyday_set = value.everyday is not None

        if everyday_set and not workdays_set and not weekend_set:
            return

        if not everyday_set and workdays_set or weekend_set:
            return

        raise SimpleValidationError.from_node(
            'Either one of workdays or weekend, or only everyday '
            'trips expected',
            node
        )


class ScalarValueExtractor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def extract(self, node):
        """Extract scalar value from scalar node, convert it from `str`
        to required type, and return obtained object
        """
        pass


class FloatValueExtractor(ScalarValueExtractor):
    def extract(self, node):
        try:
            return float(node.value)
        except ValueError:
            raise SimpleValidationError.from_node(
                '"{}" is not a valid float number'.format(node.value), node
            )


class StringValueExtractor(ScalarValueExtractor):
    def extract(self, node):
        return node.value


class BoolValueExtractor(ScalarValueExtractor):
    def extract(self, node):
        if node.value == 'true':
            return True
        elif node.value == 'false':
            return False
        else:
            raise SimpleValidationError.from_node(
                '"{}" is not a valid boolean value'.format(node.value), node
            )


if __name__ == '__main__':
    main()
