import logging

from c7n.commands import policy_command
from c7n.filters import FilterValidationError
from c7n.filters.offhours import Time
from c7n.utils import type_schema

log = logging.getLogger('custodian.businesshours')


class BusinessHours(Time):

    schema = type_schema(
        'offhour', rinherit=Time.schema, required=[],
        businesshours={'type': 'string'})

    time_type = 'on'

    # Defaults and constants
    DEFAULT_TAG = "BusinessHours"
    DEFAULT_BUSINESSHOURS = "8:00-18:00 PT"
    DEFAULT_TZ = 'pt'
    DEFAULT_OFFHOUR = 18
    DEFAULT_ONHOUR = 8
    DEFAULT_WEEKENDS = True
    DEFAULT_OPTOUT = True
    DEFAULT_ACTIONS = {
        'resource_type': {
            'asg': {
                'off': 'suspend',
                'on': 'resume'
            },
            'ec2': {
                'off': 'stop',
                'on': 'start'
            },
            'rds': {
                'off': 'stop',
                'on': 'start'
            }
        }
    }

    def __init__(self, data, manager=None):
        super(BusinessHours, self).__init__(data, manager)
        self.opt_out = self.data.get('opt-out', self.DEFAULT_OPTOUT)
        self.default_businesshours = self.data.get('businesshours', self.DEFAULT_BUSINESSHOURS)
        self.DEFAULT_HR = self.DEFAULT_ONHOUR  # Temporary for tests

    def validate(self):
        """
        Really basic validation here, because we're relying upon validation
        provided by OffHour and OnHour classes.
        """
        businesshours = self.data.get("businesshours", self.DEFAULT_BUSINESSHOURS)
        if not businesshours:
            raise FilterValidationError("Invalid businesshours specified %s" % businesshours)
        return self

    def get_default_schedule(self):
        return None

    def process_resource_schedule(self, i, value, time_type):
        on_hour, off_hour = self.parse(value)
        offhour_policies = {'policies': []}
        offhour_policies['policies'].append(PolicyBuilder('on', i[self.id_key], on_hour).policy)
        offhour_policies['policies'].append(PolicyBuilder('off', i[self.id_key], off_hour).policy)
        self.run_offhours(offhour_policies)
        return False

    @policy_command
    def run_offhours(self, policies, debug=False):
        for policy in policies:
            try:
                policy()
            except Exception:
                if debug:
                    raise
                log.exception(
                    "Error while executing policy %s, continuing" % (
                        policy.name))

    def parse(self, tag_value):
        try:
            bh_range, bh_tz = tag_value.split(" ")
            on_range, off_range = bh_range.split("-")
            on_hour, _ = on_range.split(":")  # Ignore minutes for now
            off_hour, _ = off_range.split(":")
        except ValueError:
            raise FilterValidationError(
                "Invalid BusinessHours tag specified %s" % tag_value)
        return on_hour, off_hour


class PolicyBuilder(object):

    DEFAULT_HOUR = {
        'off': 18,
        'on': 8
    }
    DEFAULT_TZ = "pt"

    BASE_POLICY = {
        'name': 'offhour-businesshours',
        'resource': 'ec2',
        'filters': [
            {'State.Name': 'running'},
            {'type': 'offhour',
             'offhour': DEFAULT_HOUR['off'],
             'tag': 'custodian_downtime',
             'default_tz': DEFAULT_TZ,
             'weekends': BusinessHours.DEFAULT_WEEKENDS}]
    }

    EXPECTED_STATE = {
        'resource_type': {
            'asg': {
                'off': 'suspended',
                'on': 'running'
            },
            'ec2': {
                'off': 'stopped',
                'on': 'running'
            },
            'rds': {
                'off': 'stopped',
                'on': 'running'
            }
        }
    }

    # self.data.get(
    #     "%shour" % self.time_type, self.DEFAULT_HR)

    def __init__(self, time_type, resource, hour):
        self.time_type = time_type
        self.resource = resource
        filter_name = "%shour" % time_type
        self.policy = {
            'name': "%s-businesshours" % filter_name,
            'resource': "%s" % resource,
            'filters': [
                {'State.Name': "%s" % self.get_expected_state()},
                {'type': "%s" % filter_name,
                 filter_name: hour,
                 'tag': 'custodian_downtime',
                 'default_tz': self.DEFAULT_TZ,
                 'weekends': BusinessHours.DEFAULT_WEEKENDS}]
        }

    def get_expected_state(self):
        return self.EXPECTED_STATE['resource_type'][self.resource][self.time_type]
