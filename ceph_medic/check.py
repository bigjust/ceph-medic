import json
import sys
import ceph_medic
import logging
from ceph_medic import runner, collector, terminal
from tambo import Transport

logger = logging.getLogger(__name__)


def as_list(string):
    if not string:
        return []
    string = string.strip(',')

    # split on commas
    string = string.split(',')

    # strip spaces
    return [x.strip() for x in string]


class Check(object):
    help = "Run checks for all the configured nodes in a cluster or hosts file"
    long_help = """
check: Run for all the configured nodes in the configuration

Options:
  --ignore              Comma-separated list of errors and warnings to ignore.
  --format              format output (term, json)

Loaded Config Path: {config_path}

Configured Nodes:
{configured_nodes}
    """

    def __init__(self, argv=None, parse=True):
        self.argv = argv or sys.argv

    @property
    def subcommand_args(self):
        # find where `check` is
        index = self.argv.index('check')
        # slice the args
        return self.argv[index:]

    def _help(self):
        node_section = []
        for daemon, node in ceph_medic.config.nodes.items():
            header = "\n* %s:\n" % daemon
            body = '\n'.join(["    %s" % n for n in ceph_medic.config.nodes[daemon]])
            node_section.append(header+body+'\n')
        return self.long_help.format(
            configured_nodes=''.join(node_section),
            config_path=ceph_medic.config.config_path
        )

    def main(self):
        options = ['--ignore', '--format']
        config_ignores = ceph_medic.config.file.get_list('check', '--ignore')
        parser = Transport(
            self.argv, options=options,
            check_version=False
        )
        parser.catch_help = self._help()
        parser.parse_args()
        ignored_codes = as_list(parser.get('--ignore', ''))
        # fallback to the configuration if nothing is defined in the CLI
        if not ignored_codes:
            ignored_codes = config_ignores

        output_format=parser.get('--format', 'term')
        if output_format not in ['term', 'json']:
            return parser.print_help()

        if len(self.argv) < 1:
            return parser.print_help()

        # populate the nodes metadata with the configured nodes
        for daemon in ceph_medic.config.nodes.keys():
            ceph_medic.metadata['nodes'][daemon] = []
        for daemon, nodes in ceph_medic.config.nodes.items():
            for node in nodes:
                node_metadata = {'host': node['host']}
                if 'container' in node:
                    node_metadata['container'] = node['container']
                ceph_medic.metadata['nodes'][daemon].append(node_metadata)

        term_enabled = True if output_format == 'term' else False

        terminal_writer = terminal._Write(enable=term_enabled)
        terminal_loader = terminal._Write(prefix='\r', clear_line=True, enable=term_enabled)

        collector.collect(term_writer=terminal_writer,
                          loader=terminal_loader)
        test = runner.Runner(ignored_codes=ignored_codes,
                             term_writer=terminal_writer,
                             term_loader=terminal_loader)
        results = test.run()

        if term_enabled:
            results.report()
        else:
            terminal._Write().write(json.dumps(ceph_medic.metadata['results']))
            
        #XXX might want to make this configurable to not bark on warnings for
        # example, setting forcefully for now, but the results object doesn't
        # make a distinction between error and warning (!)
        if results.errors or results.warnings:
            sys.exit(1)
