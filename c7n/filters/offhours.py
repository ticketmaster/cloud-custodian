# Copyright 2016 Capital One Services, LLC
# Copyright 2017 Ticketmaster & Live Nation Entertainment
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Resource Scheduling Offhours
============================

Custodian provides for time based filters, that allow for taking periodic
action on a resource, with resource schedule customization based on tag values.
A common use is offhours scheduling for asgs, and instances.

Features
========

- Flexible offhours scheduling with opt-in, opt-out selection, and timezone
  support.
- Resume during offhours support.
- Can be combined with other filters to get a particular set (
  resources with tag, vpc, etc).
- Can be combined with arbitrary actions

Policy Configuration
====================

We provide an `onhour` and `offhour` time filter, each should be used in a
different policy, they support the same configuration options

 - **weekends**: default true, whether to leave resources off for the weekend
 - **weekend-only**: default false, whether to turn the resource off only on
   the weekend
 - **default_tz**: which timezone to utilize when evaluating time
 - **tag**: default maid_offhours, which resource tag key to look for the
   resource's schedule.
 - **opt-out**: applies the default schedule to resource which do not specify
   any value.  a value of `off` to disable/exclude the resource.

The default off hours and on hours are specified per the policy configuration
along with the opt-in/opt-out behavior. Resources can specify the timezone
that they wish to have this scheduled utilized with.

We also provide a `BusinessHours` filter, which supports the configuration options:

 - **weekends**: default true, whether to leave resources off for the weekend
 - **weekend-only**: default false, whether to turn the resource off only on
   the weekend
 - **tag**: default BusinessHours, which resource tag key to look for the
   resource's schedule.
 - **default_tz**: which timezone to utilize when evaluating time, default `pt`
 - **opt-out**: applies the default schedule to resource which do not specify
   any value.  a value of `off` to disable/exclude the resource.

Tag Based Configuration
=======================

Note the tag name is configurable per policy configuration, examples below use
default tag name, ie. custodian_downtime.

- custodian_downtime:

An empty tag value implies night and weekend offhours using the default
time zone configured in the policy (tz=est if unspecified).

- custodian_downtime: tz=pt

Note all timezone aliases are referenced to a locality to ensure taking into
account local daylight savings time (if any).

- custodian_downtime: tz=Americas/Los_Angeles

A geography can be specified but must be in the time zone database.

Per http://www.iana.org/time-zones

- custodian_downtime: off

If offhours is configured to run in opt-out mode, this tag can be specified
to disable offhours on a given instance.


Policy examples
===============

# offhour / onhour

Turn ec2 instances on and off

.. code-block:: yaml

   policies:
     - name: offhours-stop
       resource: ec2
       filters:
          - type: offhour
       actions:
         - stop

     - name: offhours-start
       resource: ec2
       filters:
         - type: onhour
       actions:
         - start

Here's doing the same with auto scale groups

.. code-block:: yaml

    policies:
      - name: asg-offhours-stop
        resource: asg
        filters:
           - offhour
        actions:
           - suspend
      - name: asg-onhours-start
        resource: asg
        filters:
           - onhour
        actions:
           - resume

# BusinessHours

Turn ec2 instances on and off

.. code-block:: yaml

   policies:
     - name: businesshours
       resource: ec2
       filters:
          - type: BusinessHours

Here's doing the same with auto scale groups

.. code-block:: yaml

    policies:
      - name: asg-businesshours
        resource: asg
        filters:
           - BusinessHours

Options
=======

- tag: the tag name to use when configuring
- default_tz: the default timezone to use when interpreting offhours
- offhour: the time to turn instances off, specified in 0-23
- onhour: the time to turn instances on, specified in 0-23
- opt-out: default behavior is opt in, as in ``tag`` must be present,
  with opt-out: true, the tag doesn't need to be present.


# offhours / onhours

.. code-block:: yaml

   policies:
     - name: offhours-stop
       resource: ec2
       filters:
         - type: offhour
           tag: downtime
           onhour: 8
           offhour: 20

# BusinessHours

.. code-block:: yaml

   policies:
     - name: businesshours
       resource: ec2
       filters:
         - type: BusinessHours
           tag: bizhours
           onhour: 8
           offhour: 20

"""

# note we have to module import for our testing mocks
import datetime
import logging
from os.path import join

from collections import defaultdict
from dateutil import zoneinfo

from c7n.filters import Filter, FilterValidationError
from c7n.utils import type_schema, dumps

log = logging.getLogger('custodian.offhours')

"""
    Constants
"""
# TT = time_type
TT_ON = 'on'
TT_OFF = 'off'
TT_BIZ = 'biz'

RANGE_START = 'start'
RANGE_END = 'end'

DAYS = 'days'
HOUR = 'hour'
MINUTE = 'minute'
SECOND = 'second'
TZ = 'tz'


class Time(Filter):

    schema = {
        'type': 'object',
        'properties': {
            'tag': {'type': 'string'},
            'default_tz': {'type': 'string'},
            'weekends': {'type': 'boolean'},
            'weekends-only': {'type': 'boolean'},
            'opt-out': {'type': 'boolean'},
        }
    }

    time_type = None

    # Defaults and constants
    DEFAULT_TAG = "maid_offhours"
    DEFAULT_TZ = 'et'

    TZ_ALIASES = {
        'pdt': 'America/Los_Angeles',
        'pt': 'America/Los_Angeles',
        'pst': 'America/Los_Angeles',
        'est': 'America/New_York',
        'edt': 'America/New_York',
        'et': 'America/New_York',
        'cst': 'America/Chicago',
        'cdt': 'America/Chicago',
        'ct': 'America/Chicago',
        'mt': 'America/Denver',
        'gmt': 'Etc/GMT',
        'gt': 'Etc/GMT',
        'bst': 'Europe/London',
        'ist': 'Europe/Dublin',
        'cet': 'Europe/Berlin',
        # Technically IST (Indian Standard Time), but that's the same as Ireland
        'it': 'Asia/Kolkata',
        'jst': 'Asia/Tokyo',
        'kst': 'Asia/Seoul',
        'sgt': 'Asia/Singapore',
        'aet': 'Australia/Sydney',
        'brt': 'America/Sao_Paulo'
    }

    def __init__(self, data, manager=None):
        super(Time, self).__init__(data, manager)
        self.default_tz = self.data.get('default_tz', self.DEFAULT_TZ)
        self.weekends = self.data.get('weekends', True)
        self.weekends_only = self.data.get('weekends-only', False)
        self.opt_out = self.data.get('opt-out', False)
        self.tag_key = self.data.get('tag', self.DEFAULT_TAG).lower()
        self.default_schedule = self.get_default_schedule()
        self.parser = ScheduleParser(self.default_schedule)

        self.id_key = None

        self.opted_out = []
        self.parse_errors = []
        self.enabled_count = 0

    def validate(self):
        if self.get_tz(self.default_tz) is None:
            raise FilterValidationError(
                "Invalid timezone specified %s" % self.default_tz)
        hour = self.data.get("%shour" % self.time_type, self.default_hour)
        if hour not in self.parser.VALID_HOURS:
            raise FilterValidationError("Invalid hour specified %s" % hour)
        minute = self.data.get("%sminute" % self.time_type, self.default_minute)
        if minute not in self.parser.VALID_MINUTES:
            raise FilterValidationError("Invalid minute specified %s" % minute)
        return self

    def process(self, resources, event=None):
        resources = super(Time, self).process(resources)
        if self.parse_errors and self.manager and self.manager.log_dir:
            self.log.warning("parse errors %d", len(self.parse_errors))
            with open(join(
                    self.manager.log_dir, 'parse_errors.json'), 'w') as fh:
                dumps(self.parse_errors, fh=fh)
            self.parse_errors = []
        if self.opted_out and self.manager and self.manager.log_dir:
            self.log.debug("disabled count %d", len(self.opted_out))
            with open(join(
                    self.manager.log_dir, 'opted_out.json'), 'w') as fh:
                dumps(self.opted_out, fh=fh)
            self.opted_out = []
        return resources

    def __call__(self, i):
        value = self.get_tag_value(i)
        # Sigh delayed init, due to circle dep, process/init would be better
        # but unit testing is calling this direct.
        if self.id_key is None:
            self.id_key = (
                self.manager is None and 'InstanceId' or self.manager.get_model().id)

        # The resource tag is not present, if we're not running in an opt-out
        # mode, we're done.
        if value is False:
            if not self.opt_out:
                return False
            value = ""  # take the defaults

        # Resource opt out, track and record
        if TT_OFF == value:
            self.opted_out.append(i)
            return False
        else:
            self.enabled_count += 1

        try:
            return self.process_resource_schedule(i, value, self.time_type)
        except:
            log.exception(
                "%s failed to process resource:%s value:%s",
                self.__class__.__name__, i[self.id_key], value)
            return False

    def process_resource_schedule(self, i, value, time_type):
        """Does the resource tag schedule and policy match the current time."""
        rid = i[self.id_key]
        # this is to normalize trailing semicolons which when done allows
        # dateutil.parser.parse to process: value='off=(m-f,1);' properly.
        # before this normalization, some cases would silently fail.
        value = ';'.join(filter(None, value.split(';')))
        if self.parser.has_resource_schedule(value, time_type):
            schedule = self.parser.parse(value)
        elif self.parser.keys_are_valid(value):
            # respect timezone from tag
            raw_data = self.parser.raw_data(value)
            if TZ in raw_data:
                schedule = dict(self.default_schedule)
                schedule[TZ] = raw_data[TZ]
            else:
                schedule = self.default_schedule
        else:
            schedule = None

        if schedule is None:
            log.warning(
                "Invalid schedule on resource:%s value:%s", rid, value)
            self.parse_errors.append((rid, value))
            return False

        tz = self.get_tz(schedule[TZ])
        if not tz:
            log.warning(
                "Could not resolve tz on resource:%s value:%s", rid, value)
            self.parse_errors.append((rid, value))
            return False

        now = datetime.datetime.now(tz).replace(second=0, microsecond=0)
        # return self.match(now, schedule)
        ranged_schedule = self.parser.get_ranges(schedule)
        matched_range = self.match_range(now, ranged_schedule)
        if self.time_type == TT_ON:
            return matched_range
        else:
            return not matched_range

    def is_time_in_time_period(self, start_time, end_time, qry_time):
        if start_time < end_time:
            return start_time <= qry_time < end_time
        else:  # Crosses midnight
            return qry_time >= start_time or qry_time < end_time

    def match_range(self, now, ranged_schedule):
        if not now.weekday() in ranged_schedule:
            return False
        for r in ranged_schedule[now.weekday()]:
            if self.is_time_in_time_period(r[RANGE_START], r[RANGE_END], now.time()):
                return True
        return False

    def match(self, now, schedule):
        time = schedule.get(self.time_type, ())
        for item in time:
            days, hour = item.get(DAYS), item.get(HOUR)
            if now.weekday() in days and now.hour == hour:
                return True
        return False

    def get_tag_value(self, i):
        """Get the resource's tag value specifying its schedule."""
        # Look for the tag, Normalize tag key and tag value
        found = False
        for t in i.get('Tags', ()):
            if t['Key'].lower() == self.tag_key:
                found = t['Value']
                break
        if found is False:
            return False
        # utf8, or do translate tables via unicode ord mapping
        value = found.lower().encode('utf8')
        # Some folks seem to be interpreting the docs quote marks as
        # literal for values.
        value = value.strip("'").strip('"')
        return value

    def inverse_time_type(self):
        """Returns the inverse of the current instance of time_type."""
        if self.time_type == TT_ON:
            return TT_OFF
        elif self.time_type == TT_OFF:
            return TT_ON
        else:  # unknown type to inverse
            raise NotImplementedError("Unknown inverse for given time_type: %s", self.time_type)

    @classmethod
    def get_tz(cls, tz):
        return zoneinfo.gettz(cls.TZ_ALIASES.get(tz, tz))

    def get_default_schedule(self):
        raise NotImplementedError("use subclass")


class BaseHour(Time):

    DEFAULTS = {
        TT_ON: {
            HOUR: 7,
            MINUTE: 0
        },
        TT_OFF: {
            HOUR: 19,
            MINUTE: 0
        }
    }

    default_hour = None
    default_minute = None

    # def __init__(self, data, manager=None):
    #     super(BaseHour, self).__init__(data, manager)
    #     self.default_hour = None
    #     self.default_minute = None

    def get_time_struct(self, time_type):
        time_struct = [{
            HOUR: self.data.get(
                "%shour" % time_type, self.DEFAULTS[time_type][HOUR]),
            MINUTE: self.data.get(
                "%sminute" % time_type, self.DEFAULTS[time_type][MINUTE])}]
        if self.weekends_only:
            time_struct[0][DAYS] = [4]
        elif self.weekends:
            time_struct[0][DAYS] = range(5)
        else:
            time_struct[0][DAYS] = range(7)
        return time_struct

    def get_default_schedule(self):
        default = {TZ: self.default_tz,
                   self.time_type: self.get_time_struct(self.time_type),
                   self.inverse_time_type(): self.get_time_struct(self.inverse_time_type())}
        return default


class OffHour(BaseHour):

    schema = type_schema(
        'offhour', rinherit=Time.schema, required=['offhour', 'default_tz'],
        offhour={'type': 'integer', 'minimum': 0, 'maximum': 23},
        offminute={'type': 'integer', 'minimum': 0, 'maximum': 59})
    time_type = TT_OFF
    default_hour = BaseHour.DEFAULTS[TT_OFF][HOUR]
    default_minute = BaseHour.DEFAULTS[TT_OFF][MINUTE]

    # def __init__(self, data, manager=None):
    #     super(OffHour, self).__init__(data, manager)
    #     self.default_hour = self.DEFAULT_OFF_HR
    #     self.default_minute = self.DEFAULT_OFF_MN


class OnHour(BaseHour):

    schema = type_schema(
        'onhour', rinherit=Time.schema, required=['onhour', 'default_tz'],
        onhour={'type': 'integer', 'minimum': 0, 'maximum': 23},
        onminute={'type': 'integer', 'minimum': 0, 'maximum': 59})
    time_type = TT_ON
    default_hour = BaseHour.DEFAULTS[TT_ON][HOUR]
    default_minute = BaseHour.DEFAULTS[TT_ON][MINUTE]

    # def __init__(self, data, manager=None):
    #     super(OnHour, self).__init__(data, manager)
    #     self.default_hour = self.DEFAULT_ON_HR
    #     self.default_minute = self.DEFAULT_ON_MN


class BusinessHours(BaseHour):

    schema = type_schema(
        'businesshours', rinherit=Time.schema, required=['businesshours'],
        businesshours={'type': 'string'})
    time_type = TT_BIZ

    def __init__(self, data, manager=None):
        super(BusinessHours, self).__init__(data, manager)
        self.DEFAULT_TAG = "BusinessHours"
        self.DEFAULT_TZ = "pt"


class ScheduleParser(object):
    """Parses tag values for custom on/off hours schedules.

    At the minimum the ``on`` and ``off`` values are required. Each of
    these must be separated by a ``;`` in the format described below.

    **Schedule format**::

        # up mon-fri from 7am-7pm; eastern time
        off=(M-F,18,30);on=(M-F,7)
        # up mon-fri from 6am-9pm; up sun from 10am-6pm; pacific time
        off=[(M-F,21),(U,18,30)];on=[(M-F,6,30),(U,10)];tz=pt

    **Possible values**:

        +------------+----------------------+
        | field      | values               |
        +============+======================+
        | days       | M, T, W, H, F, S, U  |
        +------------+----------------------+
        | hours      | 0, 1, 2, ..., 22, 23 |
        +------------+----------------------+
        | minutes    | 0, 1, 2, ..., 58, 59 |
        +------------+----------------------+

        Days can be specified in a range (ex. M-F).

    If the timezone is not supplied, it is assumed ET (eastern time), but this
    default can be configurable.

    **Parser output**:

    The schedule parser will return a ``dict`` or ``None`` (if the schedule is
    invalid)::

        # off=[(M-F,21),(U,18,30)];on=[(M-F,6,30),(U,10)];tz=pt
        {
          off: [
            { days: "M-F", hour: 21, minute: 0 },
            { days: "U", hour: 18, minute: 30 }
          ],
          on: [
            { days: "M-F", hour: 6, minute: 30 },
            { days: "U", hour: 10, minute: 0 }
          ],
          tz: "pt"
        }

    """

    DAY_MAP = {'m': 0, 't': 1, 'w': 2, 'h': 3, 'f': 4, 's': 5, 'u': 6}
    VALID_HOURS = tuple(range(24))
    VALID_MINUTES = tuple(range(60))

    def __init__(self, default_schedule):
        self.default_schedule = default_schedule
        self.cache = {}

    @staticmethod
    def raw_data(tag_value):
        """convert the tag to a dictionary, taking values as is

        This method name and purpose are opaque...  and not true.
        """
        data = {}
        pieces = []
        for p in tag_value.split(' '):
            pieces.extend(p.split(';'))
        # parse components
        for piece in pieces:
            kv = piece.split('=')
            # components must by key=value
            if not len(kv) == 2:
                continue
            key, value = kv
            data[key] = value
        return data

    def keys_are_valid(self, tag_value):
        """test that provided tag keys are valid"""
        for key in ScheduleParser.raw_data(tag_value):
            if key not in (TT_ON, TT_OFF, TZ):
                return False
        return True

    def parse_toggles(self, toggles, toggle_name, results):
        for toggle in toggles:
            for day in toggle[DAYS]:
                toggle_time = "%d:%d" % (toggle[HOUR], toggle[MINUTE])
                if not results or not results[day]:
                    results[day].append(
                        {toggle_name: datetime.datetime.strptime(
                            toggle_time, "%H:%M").time()})
                else:
                    for d in results[day]:
                        d[toggle_name] = datetime.datetime.strptime(
                            toggle_time, "%H:%M").time()

    def get_ranges(self, schedule):
        """convert the on&off toggle-based schedule to a more
        flexible range-based schedule.
        """
        ranged_schedule = defaultdict(list)
        self.parse_toggles(schedule[TT_ON], RANGE_START, ranged_schedule)
        self.parse_toggles(schedule[TT_OFF], RANGE_END, ranged_schedule)
        return ranged_schedule

    def parse(self, tag_value):
        # check the cache
        if tag_value in self.cache:
            return self.cache[tag_value]

        schedule = {}

        if not self.keys_are_valid(tag_value):
            return None
        # parse schedule components
        pieces = tag_value.split(';')
        for piece in pieces:
            kv = piece.split('=')
            # components must by key=value
            if not len(kv) == 2:
                return None
            key, value = kv
            if key != TZ:
                value = self.parse_resource_schedule(value)
            if value is None:
                return None
            schedule[key] = value

        # add default timezone, if none supplied or blank
        if not schedule.get(TZ):
            schedule[TZ] = self.default_schedule[TZ]

        # cache
        self.cache[tag_value] = schedule
        return schedule

    @staticmethod
    def has_resource_schedule(tag_value, time_type):
        raw_data = ScheduleParser.raw_data(tag_value)
        # note time_type is set to 'on' or 'off' and raw_data is a dict
        return time_type in raw_data

    def parse_resource_schedule(self, lexeme):
        parsed = []
        exprs = lexeme.translate(None, '[]').split(',(')
        for e in exprs:
            tokens = e.translate(None, '()').split(',')
            # custom hours must have either: two parts - (<days>, <hour>)
            #                   or three parts - (<days>, <hour>, <minute>)
            tokens_count = len(tokens)
            if 3 > tokens_count < 2:
                return None
            if tokens_count == 3:
                if not tokens[2].isdigit():
                    return None
                minute = int(tokens[2])
            else:
                minute = 0
            if not tokens[1].isdigit():
                return None
            hour = int(tokens[1])
            if hour not in self.VALID_HOURS:
                return None
            if minute not in self.VALID_MINUTES:
                return None
            days = self.expand_day_range(tokens[0])
            if not days:
                return None
            parsed.append({DAYS: days, HOUR: hour, MINUTE: minute})
        return parsed

    def expand_day_range(self, days):
        # single day specified
        if days in self.DAY_MAP:
            return [self.DAY_MAP[days]]
        day_range = [d for d in map(self.DAY_MAP.get, days.split('-'))
                     if d is not None]
        if not len(day_range) == 2:
            return None
        # support wrap around days aka friday-monday = 4,5,6,0
        if day_range[0] > day_range[1]:
            return range(day_range[0], 7) + range(day_range[1] + 1)
        return range(min(day_range), max(day_range) + 1)
