# ============================================================================
# EVCharging System - Protocol Handler (STX/DATA/ETX/LRC)
# ============================================================================

from config import STX, ETX, DELIMITER


class Protocol:
    """
    Message format: <STX><DATA><ETX><LRC>
    DATA format: field1#field2#field3...
    LRC: XOR of all bytes in MESSAGE (STX+DATA+ETX)
    """

    @staticmethod
    def calculate_lrc(data):
        """Calculate LRC (XOR of all bytes)"""
        lrc = 0
        for byte in data:
            lrc ^= byte
        return bytes([lrc])

    @staticmethod
    def encode(message):
        """
        Encode message: <STX><DATA><ETX><LRC>
        message: string like "REGISTER#CP-001#40.5#-3.1"
        """
        message_bytes = message.encode() if isinstance(message, str) else message
        data = STX + message_bytes + ETX
        lrc = Protocol.calculate_lrc(data)
        return data + lrc

    @staticmethod
    def decode(raw_data):
        """
        Decode message from <STX><DATA><ETX><LRC>
        Returns: (message_string, is_valid)
        """
        if len(raw_data) < 4:
            return None, False

        # Check STX
        if raw_data[0:1] != STX:
            return None, False

        # Find ETX
        try:
            etx_index = raw_data.index(ETX[0], 1)
        except ValueError:
            return None, False

        # Extract parts
        data_part = raw_data[1:etx_index]
        received_lrc = raw_data[etx_index + 1:etx_index + 2]

        if len(received_lrc) != 1:
            return None, False

        # Verify LRC
        message_to_check = raw_data[:etx_index + 1]
        calculated_lrc = Protocol.calculate_lrc(message_to_check)

        if calculated_lrc != received_lrc:
            return None, False

        # Decode message
        try:
            message = data_part.decode('utf-8')
            return message, True
        except Exception:
            return None, False

    @staticmethod
    def parse_message(message):
        """
        Parse decoded message into fields
        Returns: list of fields
        """
        return message.split('#')

    @staticmethod
    def build_message(*fields):
        """
        Build message from fields
        Returns: properly formatted message string
        """
        return '#'.join(str(f) for f in fields)


# Common message types
class MessageTypes:
    # CP to CENTRAL
    REGISTER = "REGISTER"                    # CP registering
    HEARTBEAT = "HEARTBEAT"                  # CP status update
    SUPPLY_START = "SUPPLY_START"           # CP starting supply
    SUPPLY_UPDATE = "SUPPLY_UPDATE"         # CP supply status
    SUPPLY_END = "SUPPLY_END"               # CP ending supply
    FAULT = "FAULT"                         # CP health fault
    RECOVERY = "RECOVERY"                   # CP recovered from fault

    # DRIVER to CENTRAL
    REQUEST_CHARGE = "REQUEST_CHARGE"       # Driver requesting charge
    QUERY_AVAILABLE_CPS = "QUERY_AVAILABLE_CPS"  # Driver querying available CPs
    END_CHARGE = "END_CHARGE"               # Driver ending charge manually
    ACKNOWLEDGE = "ACKNOWLEDGE"             # Any acknowledgement

    # CENTRAL to CP
    AUTHORIZE = "AUTHORIZE"                 # Authorization for supply
    DENY = "DENY"                           # Denial for supply
    STOP_COMMAND = "STOP_COMMAND"          # Stop CP
    RESUME_COMMAND = "RESUME_COMMAND"      # Resume CP
    END_SUPPLY = "END_SUPPLY"               # End supply session
    TICKET = "TICKET"                       # Final ticket

    # CENTRAL to DRIVER
    AVAILABLE_CPS = "AVAILABLE_CPS"         # List of available CPs

    # MONITOR to ENGINE
    HEALTH_CHECK = "HEALTH_CHECK"           # Health check request
    HEALTH_OK = "HEALTH_OK"                 # Health check response OK
    HEALTH_KO = "HEALTH_KO"                 # Health check response KO