#!/usr/bin/env python

import os, sys
import ruamel.yaml
from jinja2 import Template


def read_from_disk(filename):
    "return file contents"

    with open(filename, 'r') as file_in:
        return file_in.read()


def write_to_disk(filename, content):
    "write to file"

    with open(filename, 'w') as file_out:
        file_out.write(content)

    print "Modified: {}".format(filename)


def get_template(filename):
    "read template from file and return jinja template object"

    with open(filename) as template_file:
        return Template(template_file.read())


def configure_setup_settings(settings):

    template = get_template("config/settings.template.sh")

    # render
    content = template.render(
        core_version=settings["version"],
        roslin_core_root=os.path.abspath(settings["root"]),
        roslin_root=os.path.abspath(settings["ROSLIN_CORE_ROOT"]),
        core_mongo_host=settings["mongo"]["host"],
        core_mongo_port=settings["mongo"]["port"],
        core_mongo_database=settings["mongo"]["database"],
        core_mongo_username=settings["mongo"]["username"],
        core_mongo_password=settings["mongo"]["password"]
    )

    write_to_disk("config/settings.sh", content)

def main():
    "main function"

    if len(sys.argv) < 2:
	print "USAGE: config.py configuration_file.yaml\nconfig.py configuration_file.yaml"
	exit()

    settings = ruamel.yaml.load(
        read_from_disk(sys.argv[1]),
        ruamel.yaml.RoundTripLoader
    )

    settings['ROSLIN_CORE_ROOT'] = os.environ['ROSLIN_ROOT']

    configure_setup_settings(settings)

if __name__ == "__main__":

    main()
