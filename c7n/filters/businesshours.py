import logging

from c7n.commands import policy_command
from c7n.filters import FilterValidationError
from c7n.filters.offhours import Time
from c7n.utils import type_schema
from collections import namedtuple
from c7n import utils

log = logging.getLogger('custodian.businesshours')

# Constants
TZ = 'tz'
OFFHOUR = 'offhour'
ONHOUR = 'onhour'
BIZHOURS = 'businesshours'
# TT = time_type
TT_ON = 'on'
TT_OFF = 'off'


class BusinessHours(Time):

    schema = type_schema(
        OFFHOUR, rinherit=Time.schema, required=[],
        businesshours={'type': 'string'})

    time_type = TT_ON

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
                TT_OFF: 'suspend',
                TT_ON: 'resume'
            },
            'ec2': {
                TT_OFF: 'stop',
                TT_ON: 'start'
            },
            # 'rds': {
            #     TT_OFF: 'stop',
            #     TT_ON: 'start'
            # }
        }
    }

    def __init__(self, data, manager=None):
        super(BusinessHours, self).__init__(data, manager)
        self.opt_out = self.data.get('opt-out', self.DEFAULT_OPTOUT)
        self.default_businesshours = self.data.get(BIZHOURS, self.DEFAULT_BUSINESSHOURS)
        self.DEFAULT_HR = self.DEFAULT_ONHOUR  # Temporary for tests

    def process(self, resources, event=None):
        resources = super(BusinessHours, self).process(resources)
        return resources

    def validate(self):
        """
        Really basic validation here, because we're relying upon validation
        provided by OffHour and OnHour classes.
        """
        businesshours = self.data.get(BIZHOURS, self.DEFAULT_BUSINESSHOURS)
        if not businesshours:
            raise FilterValidationError("Invalid businesshours specified %s" % businesshours)
        return self

    def get_default_schedule(self):
        return None

    def process_resource_schedule(self, i, value, time_type):
        bh_parsed = self.parse(value)
        offhours_tags = self.get_offhours_tags()
        # self.tag_resource(i, offhours_tags)
        # for resource in self.DEFAULT_ACTIONS['resource_type'].keys():
        #     for time_type, hour in zip(
        #             [TT_ON, TT_OFF], [bh_parsed.on_hour, bh_parsed.off_hour]):
        #         offhour_policies['policies'].append(PolicyBuilder(
        #             time_type, resource, hour, bh_parsed.tz).policy)
        # self.run_offhours(offhour_policies)
        return False

    def get_offhours_tags(self):
        """
        Very simple helper function right now. Gives the framework for more
        complexity later, should we desire to make BusinessHours more
        configurable (ie: weekends rules)
        :return: dict {'on'"(m-f,
        """
        return []

    def tag_resource(self, resource, tags):
        client = utils.local_session(
            self.manager.session_factory).client('ec2')

        self.manager.retry(
            client.create_tags,
            Resources=[resource[self.id_key]],
            Tags=tags,
            DryRun=self.manager.config.dryrun)

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
            bh_range, bh_tz = [(s[0], s[1].lower()) for s in tag_value.split(" ")]
            on_range, off_range = bh_range.split("-")
            # Ignore minutes for now
            (on_hour, _), (off_hour, _) = \
                [(y[0], y[1]) for y in [[int(x) for x in s.split(":")]
                                        for s in [on_range, off_range]]]
        except ValueError:
            raise FilterValidationError(
                "Invalid BusinessHours tag specified %s" % tag_value)
        return namedtuple('BHParsed', [ONHOUR, OFFHOUR, TZ])(on_hour, off_hour, bh_tz)


class PolicyBuilder(object):

    DEFAULT_HOUR = {
        TT_OFF: 18,
        TT_ON: 8
    }
    DEFAULT_TZ = "pt"

    BASE_POLICY = {
        'name': 'offhour-businesshours',
        'resource': 'ec2',
        'filters': [
            {'State.Name': 'running'},
            {'type': OFFHOUR,
             OFFHOUR: DEFAULT_HOUR['off'],
             'tag': 'custodian_downtime',
             'default_tz': DEFAULT_TZ,
             'weekends': BusinessHours.DEFAULT_WEEKENDS}]
    }

    EXPECTED_STATE = {
        'resource_type': {
            'asg': {
                TT_OFF: 'suspended',
                TT_ON: 'running'
            },
            'ec2': {
                TT_OFF: 'stopped',
                TT_ON: 'running'
            },
            # 'rds': {
            #     TT_OFF: 'stopped',
            #     TT_ON: 'running'
            # }
        }
    }

    # self.data.get(
    #     "%shour" % self.time_type, self.DEFAULT_HR)

    def __init__(self, time_type, resource, hour, tz):
        self.time_type = time_type
        self.resource = resource
        filter_name = "%shour" % time_type
        self.policy = {
            'name': "%s-%s" % (filter_name, BIZHOURS),
            'resource': "%s" % resource,
            'filters': [
                {'State.Name': "%s" % self.get_expected_state()},
                {'type': "%s" % filter_name,
                 filter_name: hour,
                 'tag': 'custodian_downtime',
                 'default_tz': tz,
                 'weekends': BusinessHours.DEFAULT_WEEKENDS}]
        }

    def get_expected_state(self):
        return self.EXPECTED_STATE['resource_type'][self.resource][self.time_type]
