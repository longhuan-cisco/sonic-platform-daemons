"""
    FEC auto determine/correction task manager
"""

try:
    import copy
    import sys
    import threading
    import traceback
    import ast

    from swsscommon import swsscommon

    from .xcvrd_utilities import port_mapping
    from .xcvrd_utilities.xcvr_table_helper import XcvrTableHelper
except ImportError as e:
    raise ImportError(str(e) + " - required module not found")

import json
from typing import Optional

class FecDeterminer:
    """
    A class to determine the Forward Error Correction (FEC) type based on various parameters.
    """
    DEFAULT_FEC = "none"
    
    # Predefined mapping data
    FEC_MAPPING_DATA = {
        "table_1": {
            "fec_mappings": [
                {"optics_type": "40G", "fec": "none"},
                {"optics_type": "100G-DR", "fec": "none"},
                {"optics_type": "100G-FR", "fec": "none"},
                {"optics_type": "100G-LR", "fec": "none"},
                {"optics_type": "100G-LR4", "fec": "none"},
                {"optics_type": "100G-ER4", "fec": "none"},
                {"optics_type": "100G AOC", "fec": "rs"}, # TODO: Check if this is correct (should not be none?)
                {"optics_type": "400G", "fec": "rs"},
                {"optics_type": "ALL_OTHER", "fec": "rs"}
            ]
        },
        "table_2": {
            "fec_mappings": [
                {"lane_speed": 10, "num_lane": 1, "fec": "none"},
                {"lane_speed": 10, "num_lane": 4, "fec": "none"},
                {"lane_speed": 20, "num_lane": 2, "fec": "none"},
                {"lane_speed": 25, "num_lane": 1, "fec": "rs"},
                {"lane_speed": 25, "num_lane": 2, "fec": "rs"},
                {"lane_speed": 25, "num_lane": 4, "fec": "rs"},
                {"lane_speed": 25, "num_lane": 8, "fec": "rs"},
                {"lane_speed": 50, "num_lane": 1, "fec": "rs"},
                {"lane_speed": 50, "num_lane": 2, "fec": "rs"},
                {"lane_speed": 50, "num_lane": 4, "fec": "rs"},
                {"lane_speed": 50, "num_lane": 8, "fec": "rs"},
                {"lane_speed": 50, "num_lane": 16, "fec": "rs"}
            ]
        },
        "rules": {
            "default": {
                "priority_table": "table_1"
            },
            "exceptions": [
                {
                    "condition": {"lane_speed": 10, "num_lane": [1, 4]},
                    "priority_table": "table_2"
                }
            ]
        }
    }

    def __init__(self, fec_mapping_data: Optional[dict] = None):
        """
        Initializes the FecDeterminer instance.
        
        Parameters:
        - fec_mapping_data (Optional[dict]): Optional custom FEC mapping data.
        """
        self.fec_mapping_data = fec_mapping_data if fec_mapping_data is not None else self.FEC_MAPPING_DATA
        self.ALL_RELEVANT_OPTICS_TYPES = [mapping["optics_type"] for mapping in self.fec_mapping_data["table_1"]["fec_mappings"]]
    
    def load_mapping_data_from_file(self, file_path: str):
        """
        Load FEC mapping data from a JSON file.
        
        Parameters:
        - file_path (str): The path to the JSON file containing the FEC mapping data.
        """
        with open(file_path, 'r') as file:
            self.fec_mapping_data = json.load(file)
            self.ALL_RELEVANT_OPTICS_TYPES = [mapping["optics_type"] for mapping in self.fec_mapping_data["table_1"]["fec_mappings"]]
    
    def _match_optics_type(self, mapping_optics_type: str, input_optics_type: str) -> bool:
        """
        Check if the optics_type from the mapping matches the provided input_optics_type.
        
        Parameters:
        - mapping_optics_type (str): Optics type defined in the mapping data.
        - input_optics_type (str): Provided optics type to be checked.

        Returns:
        - bool: True if the optics_type matches, False otherwise.
        """
        if mapping_optics_type == "ALL_OTHER":
            return all(known_optics_type not in input_optics_type for known_optics_type in self.ALL_RELEVANT_OPTICS_TYPES)
        else:
            return mapping_optics_type in input_optics_type

    def _beautify_optics_type(self, optics_type: str) -> str:
        """
        Beautifies the optics type.

        Parameters:
        - optics_type (str): The optics type to be beautified.

        Returns:
        - str: The beautified optics type.
        """
        return optics_type.replace("BASE", "").strip()

    def determine_fec(self, lane_speed: int, num_lanes: int, optics_type: Optional[str] = None) -> str:
        """
        Determines the appropriate Forward Error Correction (FEC) type based on lane speed, number of lanes, and optics type.

        Parameters:
        - lane_speed (int): The speed of each lane in GB.
        - num_lanes (int): The total number of lanes.
        - optics_type (Optional[str]): The type of optics in use. Can be None if not applicable.

        Returns:
        - str: The recommended FEC type based on the common rules. It can be either 'none'/'rs'/'fc'.
        """
        fec_1 = fec_2 = None
        priority_table = self.fec_mapping_data["rules"]["default"]["priority_table"]

        # Check table_1 if optics_type is provided
        if optics_type:
            optics_type = self._beautify_optics_type(optics_type)
            for mapping in self.fec_mapping_data["table_1"]["fec_mappings"]:
                if self._match_optics_type(mapping["optics_type"], optics_type):
                    fec_1 = mapping["fec"]
                    break

        # Check table_2 for lane_speed and num_lanes match
        for mapping in self.fec_mapping_data["table_2"]["fec_mappings"]:
            if mapping["lane_speed"] == lane_speed and mapping["num_lane"] == num_lanes:
                fec_2 = mapping["fec"]
                break

        # Check for any exceptions that apply
        for exception in self.fec_mapping_data["rules"]["exceptions"]:
            exception_condition = exception["condition"]
            if (
                ("lane_speed" not in exception_condition or lane_speed == exception_condition["lane_speed"]) and 
                ("num_lane" not in exception_condition or num_lanes in exception_condition["num_lane"]) and 
                ("optics_type" not in exception_condition or
                 (optics_type and self._match_optics_type(exception_condition["optics_type"], optics_type)))
            ):
                priority_table = exception["priority_table"]
                break

        # Determine FEC based on priority and available data
        if priority_table == "table_1" and fec_1 is not None:
            return fec_1
        elif priority_table == "table_2" and fec_2 is not None:
            return fec_2

        if fec_1 is not None:
            return fec_1
        elif fec_2 is not None:
            return fec_2

        return self.DEFAULT_FEC

# Thread wrapper class for FEC management


class FecManagerTask(threading.Thread):
    # Subscribe to below tables in Redis DB
    XCVR_INFO_RELEVANT_FIELDS = ['type', 'media_interface_code', 'application_advertisement', 'specification_compliance']
    PORT_TBL_MAP = [
        {
            'CONFIG_DB': swsscommon.CFG_PORT_TABLE_NAME
        },
        {
            'STATE_DB': 'TRANSCEIVER_INFO',
            #'FILTER': ['type']
        }
    ]
    # Default number of channels for QSFP28/QSFP+ transceiver
    DEFAULT_NUM_CHANNELS = 4

    def __init__(self, namespaces, main_thread_stop_event, platform_chassis, helper_logger):
        threading.Thread.__init__(self)
        self.name = "SffManagerTask"
        self.exc = None
        self.task_stopping_event = threading.Event()
        self.main_thread_stop_event = main_thread_stop_event
        self.helper_logger = helper_logger
        self.platform_chassis = platform_chassis
        # port_dict holds data per port entry with logical_port_name as key, it
        # maintains local copy of the following DB fields:
        #   CONFIG_DB PORT_TABLE 'index', 'channel', 'admin_status'
        #   STATE_DB PORT_TABLE 'host_tx_ready'
        #   STATE_DB TRANSCEIVER_INFO 'type'
        # plus 'asic_id' from PortChangeEvent.asic_id (asic_id always gets
        # filled in handle_port_update_event function based on asic_context)
        # Its port entry will get deleted upon CONFIG_DB PORT_TABLE DEL.
        # Port entry's 'type' field will get deleted upon STATE_DB TRANSCEIVER_INFO DEL.
        self.port_dict = {}
        # port_dict snapshot captured in the previous event update loop
        self.port_dict_prev = {}
        self.xcvr_table_helper = XcvrTableHelper(namespaces)
        self.namespaces = namespaces
        self.fec_determiner = FecDeterminer()

    def log_notice(self, message):
        self.helper_logger.log_notice("SFF: {}".format(message))

    def log_warning(self, message):
        self.helper_logger.log_warning("SFF: {}".format(message))

    def log_error(self, message):
        self.helper_logger.log_error("SFF: {}".format(message))

    def on_port_update_event(self, port_change_event):
        if (port_change_event.event_type
                not in [port_change_event.PORT_SET, port_change_event.PORT_DEL]):
            return

        lport = port_change_event.port_name
        pport = port_change_event.port_index
        asic_id = port_change_event.asic_id

        # Skip if it's not a physical port
        if not lport.startswith('Ethernet'):
            return

        # Skip if the physical index is not available
        if pport is None:
            return

        if port_change_event.port_dict is None:
            return

        if port_change_event.event_type == port_change_event.PORT_SET:
            if lport not in self.port_dict:
                self.port_dict[lport] = {}
            if pport >= 0:
                self.port_dict[lport]['index'] = pport
            # This field comes from CONFIG_DB PORT_TABLE. This is the channel
            # that blongs to this logical port, 0 means all channels. tx_disable
            # API needs to know which channels to disable/enable for a
            # particular physical port.
            if port_change_event.db_name == 'CONFIG_DB':
                if 'speed' in port_change_event.port_dict:
                    self.port_dict[lport]['speed'] = port_change_event.port_dict['speed']
                if 'lanes' in port_change_event.port_dict:
                    lanes = port_change_event.port_dict['lanes']
                    self.port_dict[lport]['num_lanes'] = len(lanes.split(","))

            # This field comes from STATE_DB TRANSCEIVER_INFO table.
            # TRANSCEIVER_INFO has the same life cycle as a transceiver, if
            # transceiver is inserted/removed, TRANSCEIVER_INFO is also
            # created/deleted. Thus this filed can used to determine
            # insertion/removal event.
            for field in self.XCVR_INFO_RELEVANT_FIELDS:
                if field in port_change_event.port_dict:
                    self.port_dict[lport][field] = port_change_event.port_dict[field]
            self.port_dict[lport]['asic_id'] = asic_id
        # CONFIG_DB PORT_TABLE DEL case:
        elif port_change_event.db_name and \
                port_change_event.db_name == 'CONFIG_DB':
            # Only when port is removed from CONFIG, we consider this entry as deleted.
            if lport in self.port_dict:
                del self.port_dict[lport]
        # STATE_DB TRANSCEIVER_INFO DEL case:
        elif port_change_event.table_name and \
                port_change_event.table_name == 'TRANSCEIVER_INFO':
            # TRANSCEIVER_INFO DEL corresponds to transceiver removal (not
            # port/interface removal), in this case, remove 'type' field from
            # self.port_dict
            if lport in self.port_dict and 'type' in self.port_dict[lport]:
                del self.port_dict[lport]['type']

    def get_host_tx_status(self, lport, asic_index):
        host_tx_ready = 'false'

        state_port_tbl = self.xcvr_table_helper.get_state_port_tbl(asic_index)

        found, port_info = state_port_tbl.get(lport)
        if found and 'host_tx_ready' in dict(port_info):
            host_tx_ready = dict(port_info)['host_tx_ready']
        return host_tx_ready

    def get_admin_status(self, lport, asic_index):
        admin_status = 'down'

        cfg_port_tbl = self.xcvr_table_helper.get_cfg_port_tbl(asic_index)

        found, port_info = cfg_port_tbl.get(lport)
        if found and 'admin_status' in dict(port_info):
            admin_status = dict(port_info)['admin_status']
        return admin_status
    
    def set_fec(self, lport, fec, asic_index):
        cfg_port_tbl = self.xcvr_table_helper.get_cfg_port_tbl(asic_index)

        fvs = swsscommon.FieldValuePairs([('fec', fec)])
        cfg_port_tbl.set(lport, fvs)

    def run(self):
        if self.platform_chassis is None:
            self.log_notice("Platform chassis is not available, stopping...")
            return

        try:
            self.task_worker()
        except Exception as e:
            self.helper_logger.log_error("Exception occured at {} thread due to {}".format(
                threading.current_thread().getName(), repr(e)))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            msg = traceback.format_exception(exc_type, exc_value, exc_traceback)
            for tb_line in msg:
                for tb_line_split in tb_line.splitlines():
                    self.helper_logger.log_error(tb_line_split)
            self.exc = e
            self.main_thread_stop_event.set()

    def join(self):
        self.task_stopping_event.set()
        threading.Thread.join(self)
        if self.exc:
            raise self.exc
    
    def determine_optics_type(self, sfp_info_dict):
        if not sfp_info_dict:
            return 'N/A'
        if 'type' in sfp_info_dict and sfp_info_dict['type'] in {"QSFP-DD Double Density 8X Pluggable Transceiver", "QSFP+ or later with CMIS"}:
            value = sfp_info_dict['media_interface_code']
            if 'Copper cable' == value:
                try:
                    appl_advert_dict = ast.literal_eval(sfp_info_dict['application_advertisement'])
                    value += ', ' + appl_advert_dict[1]['host_electrical_interface_id']
                except (KeyError, ValueError):
                    pass
        elif 'specification_compliance' in sfp_info_dict:
            try:
                spec_compliance_dict = ast.literal_eval(sfp_info_dict['specification_compliance'])
                if spec_compliance_dict['10/40G Ethernet Compliance Code'] == 'Extended':
                    value = spec_compliance_dict['Extended Specification Compliance']
                else:
                    value = spec_compliance_dict['10/40G Ethernet Compliance Code']
            except (KeyError, ValueError):
                value = 'N/A'
        else:
            value = 'N/A'
        return value
    
    def task_worker(self):
        '''
        Main logic of FEC auto determine/correction
        '''

        # CONFIG updates, and STATE_DB for insertion/removal, and host_tx_ready change
        sel, asic_context = port_mapping.subscribe_port_update_event(self.namespaces, self,
                                                                     self.PORT_TBL_MAP)


        while not self.task_stopping_event.is_set():
            # Internally, handle_port_update_event will block for up to
            # SELECT_TIMEOUT_MSECS until a message is received(in select
            # function). A message is received when there is a Redis SET/DEL
            # operation in the DB tables. Upon process restart, messages will be
            # replayed for all fields, no need to explictly query the DB tables
            # here.
            if not port_mapping.handle_port_update_event(
                    sel, asic_context, self.task_stopping_event, self, self.on_port_update_event):
                # In the case of no real update, go back to the beginning of the loop
                continue

            for lport in list(self.port_dict.keys()):
                if self.task_stopping_event.is_set():
                    break
                data = self.port_dict[lport]
                pport = int(data.get('index', '-1'))
                xcvr_type = data.get('type', None)
                xcvr_inserted = False
                if pport < 0:
                    continue

                if xcvr_type is None:
                    # TRANSCEIVER_INFO table's 'type' is not ready, meaning xcvr is not present
                    continue

                # Check if there's a diff between current and previous 'type'
                # It's a xcvr insertion case if TRANSCEIVER_INFO 'type' doesn't exist
                # in previous port_dict sanpshot
                if lport not in self.port_dict_prev or 'type' not in self.port_dict_prev[lport]:
                    xcvr_inserted = True

                # Skip if neither of below cases happens:
                # xcvr insertion
                if (not xcvr_inserted):
                    continue
                self.log_notice(("{}: xcvr=present(inserted={}), ").format(
                    lport,
                    xcvr_inserted))

                # double-check the HW presence before moving forward
                sfp = self.platform_chassis.get_sfp(pport)
                if not sfp.get_presence():
                    self.log_error("{}: module not present!".format(lport))
                    del self.port_dict[lport]
                    continue
                
                speed = int(data.get('speed', '-1'))
                num_lanes = int(data.get('num_lanes', '-1'))
                if speed == -1 or num_lanes == -1:
                    self.log_error(("{}: speed={}, num_lanes={} skip this port").format(
                        lport,
                        speed,
                        num_lanes))
                    continue
                optics_type = self.determine_optics_type(data)
                fec = self.fec_determiner.determine_fec(int(speed/1000/num_lanes), num_lanes, optics_type)
                self.log_notice(("{}: fec is determined as {} for speed={}, num_lanes={}, optics_type={}").format(
                    lport,
                    fec,
                    speed,
                    num_lanes,
                    optics_type))
                self.set_fec(lport, fec, data['asic_id'])

            # Take a snapshot for port_dict, this will be used to calculate diff
            # later in the while loop to determine if there's really a value
            # change on the fields related to the events we care about.
            self.port_dict_prev = copy.deepcopy(self.port_dict)