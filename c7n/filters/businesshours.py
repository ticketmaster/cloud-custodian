import logging

from c7n.filters import FilterValidationError
from c7n.filters.offhours import Time

log = logging.getLogger('custodian.businesshours')


class BusinessHours(Time):

    schema = {
        'type': 'object',
        'properties': {
            'tag': {'type': 'string'},
            'default-businesshours': {'type': 'string'},
            'weekends': {'type': 'boolean'},
            'opt-out': {'type': 'boolean'},
        }
    }

    # Defaults and constants
    DEFAULT_TAG = "BusinessHours"
    DEFAULT_BUSINESSHOURS = "8:00-18:00 PT"
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
        self.weekends = self.data.get('weekends', True)
        self.opt_out = self.data.get('opt-out', True)
        self.tag_key = self.data.get('tag', self.DEFAULT_TAG).lower()
        self.default_businesshours = self.data.get('default-businesshours', self.DEFAULT_BUSINESSHOURS)

    def process_resource_schedule(self, i, value, time_type):
        on_hour, off_hour = self.parse(value)
        # TODO: figure out how to directly hit Resource + Filter invocation with a set of custom actions
        return False

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
