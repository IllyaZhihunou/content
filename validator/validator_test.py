# coding: utf-8

import pytest

from validator import *


class StringYamlNodeSource(YamlNodeSource):
    def __init__(self, documents):
        self._documents = documents

    def enumerate(self):
        return (Yaml.create_root_node(x) for x in self._documents)


class TestNonEmptyContentValidator:
    def test_no_stops_fails(self):
        routes = [
            '''
            routes:
              - number: 1
                description: description1
                stops:
                  - key: key1
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
            '''
        ]

        with pytest.raises(EmptyContentError) as ex_info:
            self.validate([], routes)
        assert 'No stops found' in str(ex_info)

    def validate(self, stops, routes):
        content = Content(
            StringYamlNodeSource(stops), StringYamlNodeSource(routes)
        )

        NonEmptyContentValidator().validate(content)

    def test_no_routes_fails(self):
        stops = [
            '''
            stops:
              - key: key1
                name: name2
                latitude: 55.5418
                longitude: 28.666802
            '''
        ]

        with pytest.raises(EmptyContentError) as ex_info:
            self.validate(stops, [])
        assert 'No routes found' in str(ex_info)

    def test_with_stops_and_routes_succeeds(self):
        routes = [
            '''
            routes:
              - number: 1
                description: description1
                stops:
                  - key: key1
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
            '''
        ]
        stops = [
            '''
            stops:
              - key: key1
                name: name2
                latitude: 55.5418
                longitude: 28.666802
            '''
        ]

        self.validate(stops, routes)


class TestStopKeyReferentialIntegrityValidator():
    def test_valid_succeeds(self):
        stops = [
            '''
            stops:
              - key: key1
                name: name2
                latitude: 55.5418
                longitude: 28.666802
            '''
        ]
        routes = [
            '''
            routes:
              - number: 1
                description: description1
                stops:
                  - key: key1
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
            '''
        ]

        self.validate(stops, routes)

    def validate(self, stops, routes):
        content = Content(
            StringYamlNodeSource(stops), StringYamlNodeSource(routes)
        )

        StopKeyReferentialIntegrityValidator().validate(content)

    def test_invalid_fails(self):
        stops = [
            '''
            stops:
              - key: key1
                name: name2
                latitude: 55.5418
                longitude: 28.666802
            '''
        ]
        routes = [
            '''
            routes:
              - number: 1
                description: description1
                stops:
                  - key: key2
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
            '''
        ]

        with pytest.raises(ValidationError) as ex_info:
            self.validate(stops, routes)
        assert 'Undeclared stop key' in str(ex_info)


class TestStopKeyUniquenessValidator():
    def test_valid_succeeds(self):
        stops = [
            '''
            stops:
              - key: key1
                name: name1
                latitude: 55.542185
                longitude: 28.666802
              - key: key2
                name: name2
                latitude: 55.5418
                longitude: 28.666802
            '''
        ]

        self.validate(stops)

    def validate(self, stops):
        content = Content(
            StringYamlNodeSource(stops), StringYamlNodeSource([])
        )

        StopKeyUniquenessValidator().validate(content)

    def test_invalid_fails(self):
        stops = [
            '''
            stops:
              - key: key1
                name: name1
                latitude: 55.542185
                longitude: 28.666802
              - key: key1
                name: name2
                latitude: 55.5418
                longitude: 28.666802
            '''
        ]

        with pytest.raises(ValidationError) as ex_info:
            self.validate(stops)
        assert 'Key' in str(ex_info)
        assert 'used second time' in str(ex_info)


class TestContent:
    def test_stops_from_multiple_sources(self):
        stops = [
            '''
            stops:
              - key: key1
                name: name1
                latitude: 55.542185
                longitude: 28.666802
              - key: key2
                name: name2
                latitude: 55.5418
                longitude: 28.666802
            ''',
            '''
            stops:
              - key: key3
                name: name3
                latitude: 55.550659
                longitude: 28.633479
              - key: key4
                name: name4
                latitude: 55.5503
                longitude: 28.633575
            '''
        ]

        content = Content(
            StringYamlNodeSource(stops), StringYamlNodeSource([])
        )

        assert len(content.stops) == 4
        for stop in content.stops:
            assert isinstance(stop.value, Stop)

    def test_routes_from_multiple_sources(self):
        routes = [
            '''
            routes:
              - number: 1
                description: description1
                stops:
                  - key: key1
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
              - number: 2
                description: description2
                stops:
                  - key: key2
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
            ''',
            '''
            routes:
              - number: 3
                description: description3
                stops:
                  - key: key3
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
              - number: 4
                description: description4
                stops:
                  - key: key4
                    shift: 00:00
                trips:
                  everyday:
                    - 05:59
            '''
        ]

        content = Content(
            StringYamlNodeSource([]), StringYamlNodeSource(routes)
        )

        assert len(content.routes) == 4
        for route in content.routes:
            assert isinstance(route.value, Route)


class TestRouteProducer:
    def test(self):
        yaml_doc = \
            '''
            number: 1
            description: Больничный городок → ОАО «Нафтан»
            hidden: true
            stops:
              - key: magazin-berezka-odd
                shift: 00:00
              - key: gdk-odd
                shift: 00:02
            trips:
              everyday:
                - 06:00
                - 06:10
                - 06:25
            '''

        route = RouteProducer().produce(Yaml.create_root_node(yaml_doc)).value

        assert isinstance(route, Route)
        assert route.number.value == '1'
        assert len(route.stops.value) == 2
        assert len(route.trips.value.everyday.value) == 3
        assert route.trips.value.weekend is None
        assert route.trips.value.workdays is None


class TestRouteStopProducer:
    def test(self):
        yaml_doc = \
            '''
            key: magazin-berezka-odd
            shift: 00:02
            '''

        route_stop = RouteStopProducer().produce(
            Yaml.create_root_node(yaml_doc)
        ).value

        assert isinstance(route_stop, RouteStop)
        assert route_stop.key.value == 'magazin-berezka-odd'
        assert route_stop.shift.value == '00:02'


class TestRouteTripProducer:
    def test(self):
        yaml_doc = \
            '''
            workdays:
            - 06:00
            - 06:10
            weekend:
            - 06:25
            '''

        route_trip = RouteTripProducer().produce(
            Yaml.create_root_node(yaml_doc)
        ).value

        assert isinstance(route_trip, RouteTrip)

        assert route_trip.everyday is None

        assert isinstance(route_trip.workdays.value, list)
        assert len(route_trip.workdays.value) == 2

        assert isinstance(route_trip.weekend.value, list)
        assert len(route_trip.weekend.value) == 1


class TestStopTupleProducer:
    def test(self):
        yaml_doc = \
            '''
            key: koptevo-to-borovuha
            name: Коптево
            direction: в Боровуху
            latitude: 55.542185
            longitude: 28.666802
            '''

        stop = StopProducer().produce(Yaml.create_root_node(yaml_doc)).value

        assert isinstance(stop, Stop)
        assert stop.key.value == 'koptevo-to-borovuha'
        assert stop.name.value == 'Коптево'
        assert stop.direction.value == 'в Боровуху'
        assert stop.latitude.value == 55.542185
        assert stop.longitude.value == 28.666802


class TestNamedTupleProducer:
    Model = namedtuple('Model', 'text_item, float_item, optional_bool_item')

    def test_no_optional_succeeds(self):
        yaml_doc = \
            '''
            text_item: this is some text
            float_item: 123
            '''
        item = self._make_model_producer().produce(
            Yaml.create_root_node(yaml_doc)
        )

        assert isinstance(item.value, self.Model)
        assert item.value.text_item.value == 'this is some text'
        assert item.value.float_item.value == 123
        assert item.value.optional_bool_item is None

    def _make_model_producer(self):
        return NamedTupleProducer(
            self.Model,
            dict(
                text_item=ScalarProducer(
                    StringValueExtractor(), NonEmptyStringValidator()
                ),
                float_item=ScalarProducer(FloatValueExtractor())
            ),
            dict(optional_bool_item=ScalarProducer(BoolValueExtractor()))
        )

    def test_with_optional_succeeds(self):
        yaml_doc = \
            '''
            text_item: this is some text
            float_item: 123
            optional_bool_item: true
            '''
        item = self._make_model_producer().produce(
            Yaml.create_root_node(yaml_doc)
        )

        assert isinstance(item.value, self.Model)
        assert item.value.text_item.value == 'this is some text'
        assert item.value.float_item.value == 123
        assert item.value.optional_bool_item.value == True

    def test_with_no_required_fails(self):
        yaml_doc = \
            '''
            text_item: this is some text
            '''

        with pytest.raises(DataError) as ex_info:
            self._make_model_producer().produce(
                Yaml.create_root_node(yaml_doc)
            )

        assert 'Required item' in str(ex_info)
        assert 'not specified' in str(ex_info)

    def test_no_map_fails(self):
        yaml_doc = \
            '''
            - 123
            - 321
            '''

        with pytest.raises(DataError) as ex_info:
            self._make_model_producer().produce(
                Yaml.create_root_node(yaml_doc)
            )

        assert 'Mapping expected' in str(ex_info)


class TestListProducer:
    def test_valid_succeeds(self):
        producer = ListProducer(
            ScalarProducer(StringValueExtractor(), NonEmptyStringValidator())
        )
        yaml_doc = \
            '''
            - one
            - two
            - three
            '''

        item = producer.produce(Yaml.create_root_node(yaml_doc))

        assert isinstance(item.value, list)
        for list_item in item.value:
            assert isinstance(list_item, Item)
            assert isinstance(list_item.value, str)
        assert item.start_mark.line == 1
        assert item.start_mark.column == 12  # note indenting
        assert item.end_mark.line == 4
        assert item.start_mark.column == 12  # note indenting

    def test_non_list_fails(self):
        producer = ListProducer(
            ScalarProducer(StringValueExtractor(), NonEmptyStringValidator())
        )
        yaml_doc = '123'

        with pytest.raises(DataError) as ex_info:
            producer.produce(Yaml.create_root_node(yaml_doc))

        assert 'Sequence expected' in str(ex_info)


class TestScalarProducer:
    def test_valid_succeeds(self):
        producer = ScalarProducer(
            StringValueExtractor(),
            NonEmptyStringValidator()
        )
        node = Yaml.create_root_node('some text')

        item = producer.produce(node)

        assert item.value == 'some text'
        assert item.start_mark.line == 0
        assert item.start_mark.column == 0
        assert item.end_mark.line == 0
        assert item.end_mark.column == 9

    def test_non_scalar_fails(self):
        producer = ScalarProducer(StringValueExtractor())
        node = Yaml.create_root_node('lorem: ipsum')

        with pytest.raises(DataError) as ex_info:
            producer.produce(node)

        assert 'Scalar expected' in str(ex_info)


class TestStringKeyValidator:
    def test_valid(self):
        node = Yaml.create_root_node('valid-key')
        StringKeyValidator().validate(node.value, node)

    def test_invalid(self):
        node = Yaml.create_root_node('invalid_key')
        with pytest.raises(DataError) as ex_info:
            StringKeyValidator().validate(node.value, node)
        assert 'Invalid character' in str(ex_info)


class TestFloatRangeValidator:
    def test_in_between_succeeds(self):
        node = Yaml.create_root_node('123.45')
        value = FloatValueExtractor().extract(node)
        FloatRangeValidator(10.1, 200.2).validate(value, node)

    def test_equal_succeeds(self):
        node = Yaml.create_root_node('123.45')
        value = FloatValueExtractor().extract(node)
        FloatRangeValidator(123.45, 200.2).validate(value, node)
        FloatRangeValidator(1.2, 123.45).validate(value, node)

    def test_too_small_fails(self):
        node = Yaml.create_root_node('1.0')
        value = FloatValueExtractor().extract(node)

        with pytest.raises(DataError) as ex_info:
            FloatRangeValidator(10.1, 200.2).validate(value, node)
        assert 'expected to be in' in str(ex_info)
        assert 'interval' in str(ex_info)

    def test_too_big_fails(self):
        node = Yaml.create_root_node('1000.0')
        value = FloatValueExtractor().extract(node)

        with pytest.raises(DataError) as ex_info:
            FloatRangeValidator(10.1, 200.2).validate(value, node)
        assert 'expected to be in' in str(ex_info)
        assert 'interval' in str(ex_info)


class TestTimeShiftValidator:
    def test_regular_time_succeeds(self):
        self._validate('10:12')

    def _validate(self, yaml_doc):
        node = Yaml.create_root_node(yaml_doc)
        value = StringValueExtractor().extract(node)
        StringTimeShiftValidator().validate(value, node)

    def test_overflow_time_fails(self):
        with pytest.raises(DataError) as ex_info:
            self._validate('00:72')
        self._assert_not_valid_time_error(ex_info)

    def _assert_not_valid_time_error(self, ex_info):
        assert 'is not a valid time' in str(ex_info)

    def test_date_and_time_fails(self):
        with pytest.raises(DataError) as ex_info:
            self._validate('01.12.2015 00:13')
        self._assert_not_valid_time_error(ex_info)

    def test_non_time_fails(self):
        with pytest.raises(DataError) as ex_info:
            self._validate('some string')
        self._assert_not_valid_time_error(ex_info)


class BaseTestValueExtractor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _get_extractor(self):
        pass

    def _extract_node_value(self, yaml_doc):
        node = self._make_scalar_node(yaml_doc)
        return self._get_extractor().extract(node)

    def _make_scalar_node(self, yaml_doc):
        root = Yaml.create_root_node(yaml_doc)
        return root.value[0][1]


class TestFloatValueExtractor(BaseTestValueExtractor):
    def _get_extractor(self):
        return FloatValueExtractor()

    def test_int_number_succeeds(self):
        assert self._extract_node_value('value: 123') == 123.0

    def test_float_number_succeeds(self):
        assert self._extract_node_value('value: 123.45') == 123.45

    def test_negative_succeeds(self):
        assert self._extract_node_value('value: -123.45') == -123.45

    def test_empty_fails(self):
        with pytest.raises(DataError) as ex_info:
            self._extract_node_value('value:')
        self._assert_not_valid_float_number_error(ex_info)

    def _assert_not_valid_float_number_error(self, ex_info):
        assert 'is not a valid float number' in str(ex_info)

    def test_non_float_number_fails(self):
        with pytest.raises(DataError) as ex_info:
            self._extract_node_value('value: some string here')
        self._assert_not_valid_float_number_error(ex_info)


class TestStringValueExtractor(BaseTestValueExtractor):
    def _get_extractor(self):
        return StringValueExtractor()

    def test(self):
        assert self._extract_node_value('value: some value') == 'some value'


class TestBoolValueExtractor(BaseTestValueExtractor):
    def _get_extractor(self):
        return BoolValueExtractor()

    def test_true_succeeds(self):
        assert self._extract_node_value('value: true') == True

    def test_false_succeeds(self):
        assert self._extract_node_value('value: false') == False

    def test_empty_fails(self):
        with pytest.raises(DataError) as ex_info:
            self._extract_node_value('value:')
        self._assert_not_valid_bool_error(ex_info)

    def _assert_not_valid_bool_error(self, ex_info):
        assert 'is not a valid boolean value' in str(ex_info)

    def test_invalid_string_fails(self):
        with pytest.raises(DataError) as ex_info:
            self._extract_node_value('value: lorem ipsum')
        self._assert_not_valid_bool_error(ex_info)


class TestPyyamlInterface:
    def test_scalar_node(self):
        yaml_doc = 'test'
        root = Yaml.create_root_node(yaml_doc)

        assert isinstance(root, yaml.ScalarNode)
        assert root.value == 'test'

    def test_sequence_node(self):
        yaml_doc = \
            '''
            - one
            - two
            - three
            '''
        root = Yaml.create_root_node(yaml_doc)

        assert isinstance(root, yaml.SequenceNode)
        assert len(root.value) == 3
        for item in root.value:
            assert isinstance(item, yaml.ScalarNode)

    def test_mapping_node(self):
        yaml_doc = \
            '''
            key_one: value_one
            key_two: value_two
            key_three: value_three
            '''

        root = Yaml.create_root_node(yaml_doc)

        assert isinstance(root, yaml.MappingNode)
        assert len(root.value) == 3
        for item in root.value:
            assert isinstance(item, tuple)
            assert len(item) == 2

    def test_invalid_yaml(self):
        with pytest.raises(YamlFormatError) as ex_info:
            Yaml.create_root_node(']')
        assert 'YAML parsing error' in str(ex_info)
