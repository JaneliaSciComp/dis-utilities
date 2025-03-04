''' update_name.py
    Update the name in a single record in the orcid table.
    The record may be selected by ORCID or by given and family name.
'''

__version__ = '1.0.0'

import argparse
import copy
import json
import sys
from operator import attrgetter
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout,
                             QListWidget, QLineEdit, QPushButton, QLabel, QMessageBox)
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# General
ARG = LOGGER = None
BTN = {}

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.prod.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'prod', dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def create_gui(rec):
    """ Create and configure the GUI window and its components
        Keyword arguments:
          rec: record to update
        Returns:
          None
    """
    initial_data = {'current': rec, 'original': copy.deepcopy(rec)}
    app = QApplication(sys.argv)
    window = QWidget()
    window.setWindowTitle('Name Editor')
    layout = QVBoxLayout()
    # Create list widgets
    given_list = QListWidget()
    given_list.addItems(initial_data['current']['given'])
    family_list = QListWidget()
    family_list.addItems(initial_data['current']['family'])
    # Configure layouts
    lists_layout = QHBoxLayout()
    lists_layout.addWidget(QLabel('Given Names:'))
    lists_layout.addWidget(given_list)
    lists_layout.addWidget(QLabel('Family Names:'))
    lists_layout.addWidget(family_list)
    layout.addLayout(lists_layout)
    name_input = QLineEdit()
    name_input.setPlaceholderText('Enter name here...')
    layout.addWidget(name_input)
    # Configure save, revert, and cancel buttons
    BTN['save'] = QPushButton('Save')
    BTN['save'].setFixedWidth(100)
    BTN['save'].setEnabled(False)
    BTN['save'].setStyleSheet("""
        QPushButton {
            background-color: #888888;
            color: white;
            padding: 5px;
            border: none;
            border-radius: 3px;
            min-height: 20px;
        }
        QPushButton:enabled {
            background-color: #2eb82e;
        }
        QPushButton:enabled:hover {
            background-color: #33cc33;
        }
    """)
    BTN['revert'] = QPushButton('Revert')
    BTN['revert'].setFixedWidth(100)
    BTN['revert'].setEnabled(False)
    BTN['revert'].setStyleSheet("""
        QPushButton {
            background-color: #888888;
            color: white;
            padding: 5px;
            border: none;
            border-radius: 3px;
            min-height: 20px;
        }
        QPushButton:enabled {
            background-color: #ff9933;
        }
        QPushButton:enabled:hover {
            background-color: #ffad33;
        }
    """)
    # Configure cancel button
    BTN['cancel'] = QPushButton('Cancel')
    BTN['cancel'].setFixedWidth(100)
    BTN['cancel'].setStyleSheet("""
        QPushButton {
            background-color: #cc0000;
            color: white;
            padding: 5px;
            border: none;
            border-radius: 3px;
            min-height: 20px;
        }
        QPushButton:hover {
            background-color: #e60000;
        }
    """)

    def check_changes():
        """ Check if current data differs from original and update buttons
            Keyword arguments:
              None
            Returns:
              None
        """
        has_changes = (
            sorted(initial_data['current']['given']) !=
            sorted(initial_data['original']['given']) or
            sorted(initial_data['current']['family']) !=
            sorted(initial_data['original']['family'])
        )
        BTN['save'].setEnabled(has_changes)
        BTN['revert'].setEnabled(has_changes)

    def add_name(field):
        """ Add a name to the specified field
            Keyword arguments:
              field: field to add name to
            Returns:
              None
        """
        name = name_input.text().strip()
        if name and name not in initial_data['current'][field]:
            initial_data['current'][field].append(name)
            if field == 'given':
                given_list.addItem(name)
            else:
                family_list.addItem(name)
            name_input.clear()
            check_changes()

    def delete_name(field):
        """ Delete selected name from specified field
            Keyword arguments:
              field: field to delete name from
            Returns:
              None
        """
        list_widget = given_list if field == 'given' else family_list
        item = list_widget.currentItem()
        if item:
            name = item.text()
            reply = QMessageBox.question(window, 'Confirm Delete',
                f'Are you sure you want to delete "{name}" from {field} names?',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.Yes:
                initial_data['current'][field].remove(name)
                list_widget.takeItem(list_widget.row(item))
                check_changes()

    def save_data():
        """ Save current data and update original state
            Keyword arguments:
              None
            Returns:
              None
        """
        if ARG.WRITE:
            try:
                DB['dis'].orcid.update_one({"_id": initial_data['current']['_id']},
                                           {"$set": {"given": initial_data['current']['given'],
                                                     "family": initial_data['current']['family']}}
                                          )
            except Exception as err:
                terminate_program(err)
        else:
            LOGGER.warning("Write flag not set, skipping update")
            print(json.dumps(initial_data['current'], indent=2, default=str))
        initial_data['original'] = copy.deepcopy(initial_data['current'])
        BTN['save'].setEnabled(False)
        QMessageBox.information(window, "Success", "Changes saved successfully!")

    def revert_changes():
        """ Restore the original data
            Keyword arguments:
              None
            Returns:
              None
        """
        reply = QMessageBox.question(window, 'Confirm Revert',
            'Are you sure you want to revert all changes?',
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            initial_data['current'] = copy.deepcopy(initial_data['original'])
            given_list.clear()
            family_list.clear()
            given_list.addItems(initial_data['current']['given'])
            family_list.addItems(initial_data['current']['family'])
            check_changes()
            update_delete_buttons()
            QMessageBox.information(window, "Success", "Changes reverted successfully!")

    def handle_close():
        """ Handle application close with unsaved changes check
            Keyword arguments:
              None
            Returns:
              None
        """
        if BTN['save'].isEnabled():
            reply = QMessageBox.question(
                window,
                'Unsaved Changes',
                'You have unsaved changes. Do you want to quit anyway?',
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                app.quit()
        else:
            app.quit()

    # Configure action buttons
    button_layout = QHBoxLayout()
    for txt in ['add_given', 'add_family', 'delete_given', 'delete_family']:
        BTN[txt] = QPushButton(txt.replace('_', ' ').title())
    # Initially disable delete buttons
    BTN['delete_given'].setEnabled(False)
    BTN['delete_family'].setEnabled(False)

    def validate_lists():
        """ Check if either name list is empty and disable save button if so
            Keyword arguments:
              None
            Returns:
              None
        """
        if not given_list.count() or not family_list.count():
            BTN['save'].setEnabled(False)
            QMessageBox.warning(window, "Warning", "Both given and family names are required")
        else:
            check_changes()

    def update_delete_buttons():
        """ Enable/disable delete buttons based on list selection
            Keyword arguments:
              None
            Returns:
              None
        """
        BTN['delete_given'].setEnabled(bool(given_list.currentItem()))
        BTN['delete_family'].setEnabled(bool(family_list.currentItem()))

    # Connect selection changes to button updates
    given_list.itemSelectionChanged.connect(update_delete_buttons)
    family_list.itemSelectionChanged.connect(update_delete_buttons)
    # Connect button signals
    BTN['add_given'].clicked.connect(lambda: add_name('given'))
    BTN['add_family'].clicked.connect(lambda: add_name('family'))
    BTN['delete_given'].clicked.connect(lambda: delete_name('given'))
    BTN['delete_family'].clicked.connect(lambda: delete_name('family'))
    BTN['save'].clicked.connect(save_data)
    BTN['revert'].clicked.connect(revert_changes)
    BTN['cancel'].clicked.connect(handle_close)
    # Add buttons to layout
    for txt in ['add_given', 'add_family', 'delete_given', 'delete_family']:
        button_layout.addWidget(BTN[txt])
        # Connect delete buttons to validation
        if txt.startswith('delete'):
            BTN[txt].clicked.connect(validate_lists)
    layout.addLayout(button_layout)
    # Add save/revert/cancel buttons
    action_layout = QHBoxLayout()
    action_layout.addStretch()
    for txt in ['save', 'revert', 'cancel']:
        action_layout.addWidget(BTN[txt])
    action_layout.addStretch()
    layout.addLayout(action_layout)
    window.setLayout(layout)
    window.setGeometry(300, 300, 600, 400)
    return app, window


def process():
    """Allow user to update name for a single record
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.ORCID:
        payload = {"orcid": ARG.ORCID}
    else:
        payload = {"given": ARG.GIVEN, "family": ARG.FAMILY}
    try:
        rows = DB['dis'].orcid.find(payload).collation({"locale": "en",
                                                        "strength": 1}).sort("family", 1)
        rec = []
        for row in rows:
            rec.append(row)
        if len(rec) > 1:
            LOGGER.warning(f"Multiple records found for {payload}")
    except Exception as err:
        terminate_program(err)
    if not rec:
        terminate_program(f"Record not found: {payload}")
    try:
        app, window = create_gui(rec[0])
        window.show()
        sys.exit(app.exec_())
    except Exception as err:
        terminate_program(err)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update author name")
    group = PARSER.add_argument_group()
    group.add_argument('--orcid', dest='ORCID', action='store',
                        help='ORCID ID')
    group2 = PARSER.add_argument_group()
    group2.add_argument('--given', dest='GIVEN', action='store',
                        help='Given name')
    group2.add_argument('--family', dest='FAMILY', action='store',
                        help='Family name')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write changes to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    if not(ARG.ORCID or (ARG.GIVEN and ARG.FAMILY)):
        PARSER.error("Either --orcid or --given and --family must be provided")
    initialize_program()
    process()
