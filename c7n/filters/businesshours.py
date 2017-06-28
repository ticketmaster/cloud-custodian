import logging

from c7n.filters import FilterValidationError
from c7n.filters.offhours import OnHour, OffHour
from c7n.utils import type_schema, get_instance_key
from collections import namedtuple

log = logging.getLogger('custodian.businesshours')

# Constants
TZ = 'tz'
OFFHOUR = 'offhour'
ONHOUR = 'onhour'
BIZHOURS = 'BusinessHours'
T24HOURS = '24hours'
# TT = time_type
TT_ON = 'on'
TT_OFF = 'off'


class BusinessHours(object):
    # Defaults and constants
    DEFAULT_TAG = "BusinessHours"
    DEFAULT_TZ = 'pt'
    DEFAULT_OFFHOUR = 18
    DEFAULT_ONHOUR = 8
    DEFAULT_WEEKENDS = True
    DEFAULT_OPTOUT = True
    DEFAULT_BUSINESSHOURS = "8:00-18:00 PT"

    @staticmethod
    def is_24hours(value):
        if value == T24HOURS or value == '24hour':
            return True
        else:
            return False

    @staticmethod
    def parse(tag_value):
        """
        Given a BusinessHours tag, parse attempts to break it down into
        onhours, offhours, and timezone. Assumes each step is ok, and
        Pythonically fails with an exception if any step encounters
        a problem.

        :param tag_value: expects string w/ <start:time>-<end:time> <timezone>
         example: "8:00-18:00 PT"
        :return: namedtuple('BHParsed', ['onhour', 'offhour', 'tz'])
         example: BHParsed(8, 18, 'pt')
        """
        try:
            bh_range, bh_tz = tag_value.split(" ")
            bh_tz = bh_tz.lower()
            on_range, off_range = bh_range.split("-")
            # Ignore minutes for now
            (on_hour, _), (off_hour, _) = \
                [(y[0], y[1]) for y in [[int(x) for x in s.split(":")]
                                        for s in [on_range, off_range]]]
        except ValueError:
            raise FilterValidationError(
                "Invalid BusinessHours tag specified %s" % tag_value)
        return namedtuple('BHParsed', [ONHOUR, OFFHOUR, TZ])(on_hour, off_hour, bh_tz)

    def validate(self):
        """
        Really basic validation here, because we're relying upon validation
        provided by OffHour and OnHour classes.
        """
        default_onhour = self.data.get(ONHOUR, self.DEFAULT_ONHOUR)
        default_offhour = self.data.get(OFFHOUR, self.DEFAULT_OFFHOUR)
        if not default_onhour and not default_offhour:
            raise FilterValidationError("Invalid hours specified %s, %s" % (default_onhour, default_offhour))
        return self

    def get_businesshours(self):
        return "{}:00-{}:00 {}".format(self.DEFAULT_ONHOUR, self.DEFAULT_OFFHOUR, self.default_tz)


class BusinessHoursOn(BusinessHours, OnHour):
    schema = type_schema(
        'businesshours_on', rinherit=OnHour.schema)

    time_type = TT_ON

    def __init__(self, data, manager=None):
        super(BusinessHoursOn, self).__init__(data, manager)
        self.opt_out = self.data.get('opt-out', self.DEFAULT_OPTOUT)
        self.DEFAULT_ONHOUR = self.data.get(ONHOUR, self.DEFAULT_ONHOUR)
        self.bh_parsed = None

    def process_resource_schedule(self, i, value, time_type):
        if self.is_24hours(value.lower()):
            return False
        elif value == "":  # handle the default value case
            value = self.get_businesshours()
        return super(BusinessHoursOn, self).process_resource_schedule(i, value, time_type)

    # convert from 8:30-18:30 PT to off=(m-f,18);on=(m-f,8);tz=pt
    def get_tag_value(self, i):
        raw_value = super(BusinessHoursOn, self).get_tag_value(i)
        if raw_value is False:
            return False
        elif raw_value == "":
            return raw_value
        elif self.is_24hours(raw_value.lower()):
            return raw_value
        self.bh_parsed = super(BusinessHoursOn, self).parse(raw_value)
        return "{}=(m-f,{});{}=(m-f,{});tz={}".format(
            TT_OFF, self.bh_parsed.offhour, TT_ON, self.bh_parsed.onhour, self.bh_parsed.tz)


class BusinessHoursOff(BusinessHours, OffHour):
    schema = type_schema(
        'businesshours_off', rinherit=OffHour.schema)
    time_type = TT_OFF

    def __init__(self, data, manager=None):
        super(BusinessHoursOff, self).__init__(data, manager)
        self.opt_out = self.data.get('opt-out', self.DEFAULT_OPTOUT)
        self.DEFAULT_OFFHOUR = self.data.get(OFFHOUR, self.DEFAULT_OFFHOUR)
        self.bh_parsed = None

    def process_resource_schedule(self, i, value, time_type):
        if self.is_24hours(value.lower()):
            return False
        elif value == "":  # handle the default value case
            value = self.get_businesshours()
        return super(BusinessHoursOff, self).process_resource_schedule(i, value, time_type)

    # convert from 8:30-18:30 PT to off=(m-f,18);on=(m-f,8);tz=pt
    def get_tag_value(self, i):
        raw_value = super(BusinessHoursOff, self).get_tag_value(i)
        if raw_value is False:
            return False
        elif raw_value == "":
            return raw_value
        elif self.is_24hours(raw_value.lower()):
            return raw_value
        self.bh_parsed = super(BusinessHoursOff, self).parse(raw_value)
        return "{}=(m-f,{});{}=(m-f,{});tz={}".format(
            TT_OFF, self.bh_parsed.offhour, TT_ON, self.bh_parsed.onhour, self.bh_parsed.tz)
