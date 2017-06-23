import logging

from c7n.filters import FilterValidationError
from c7n.filters.offhours import OnHour, OffHour
from c7n.utils import type_schema
from collections import namedtuple

log = logging.getLogger('custodian.businesshours')

# Constants
TZ = 'tz'
OFFHOUR = 'offhour'
ONHOUR = 'onhour'
BIZHOURS = 'businesshours'
# TT = time_type
TT_ON = 'on'
TT_OFF = 'off'


class BusinessHours(object):
    # Defaults and constants
    DEFAULT_TAG = "BusinessHours"
    DEFAULT_BUSINESSHOURS = "8:00-18:00 PT"
    DEFAULT_TZ = 'pt'
    DEFAULT_OFFHOUR = 18
    DEFAULT_ONHOUR = 8
    DEFAULT_WEEKENDS = True
    DEFAULT_OPTOUT = True

    def validate(self):
        """
        Really basic validation here, because we're relying upon validation
        provided by OffHour and OnHour classes.
        """
        businesshours = self.data.get(BIZHOURS, self.DEFAULT_BUSINESSHOURS)
        if not businesshours:
            raise FilterValidationError("Invalid businesshours specified %s" % businesshours)
        return self

    def parse(self, tag_value):
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


class BusinessHoursOn(BusinessHours, OnHour):
    schema = type_schema(
        'businesshours_on', rinherit=OnHour.schema)

    time_type = TT_ON

    def __init__(self, data, manager=None):
        super(BusinessHoursOn, self).__init__(data, manager)
        self.opt_out = self.data.get('opt-out', self.DEFAULT_OPTOUT)
        self.default_businesshours = self.data.get(BIZHOURS, self.DEFAULT_BUSINESSHOURS)
        self.DEFAULT_HR = self.DEFAULT_ONHOUR  # Temporary for tests
        self.bh_parsed = None

    # convert from 8:30-18:30 PT to off=(m-f,18);on=(m-f,8);tz=pt
    def get_tag_value(self, i):
        raw_value = super(BusinessHoursOn, self).get_tag_value(i)
        if raw_value is False:
            return ""  # Use the default

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
        self.default_businesshours = self.data.get(BIZHOURS, self.DEFAULT_BUSINESSHOURS)
        self.DEFAULT_HR = self.DEFAULT_OFFHOUR  # Temporary for tests
        self.bh_parsed = None

    # convert from 8:30-18:30 PT to off=(m-f,18);on=(m-f,8);tz=pt
    def get_tag_value(self, i):
        raw_value = super(BusinessHoursOff, self).get_tag_value(i)
        if raw_value is False:
            return ""  # Use the default

        self.bh_parsed = super(BusinessHoursOff, self).parse(raw_value)
        return "{}=(m-f,{});{}=(m-f,{});tz={}".format(
            TT_OFF, self.bh_parsed.offhour, TT_ON, self.bh_parsed.onhour, self.bh_parsed.tz)
