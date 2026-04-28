# ============================================================================
# Audito žurnalo sistema
# Saugo visus įvykius struktūrizuotai pagal aprašymą
# ============================================================================

import os
import json
import threading
from datetime import datetime
from config import AUDIT_LOG_FILE, AUDIT_ENABLED

class AuditLogger:
    """
    Audito žurnalas pagal specifikaciją:
    
    Kiekvienas įrašas turi:
    a. Įvykio data ir laikas
    b. Kas ir iš kur įvyksta įvykis (IP adresas)
    c. Koks veiksmas atliekamas
    d. Įvykio parametrai arba aprašymas
    """
    
    def __init__(self, log_file=AUDIT_LOG_FILE):
        self.log_file = log_file
        self.lock = threading.Lock()
        self.enabled = AUDIT_ENABLED
        
        # Sukurti katalogą, jei neegzistuoja
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        if self.enabled:
            print(f"[AuditLogger] Enabled - logging to {log_file}")
        else:
            print(f"[AuditLogger] Disabled")
    
    def log_event(self, source_ip, event_type, action, details=None):
        """
        Įrašyti audito įvykį
        
        Args:
            source_ip: IP adresas, iš kurio įvykis (pvz., "192.168.1.10")
            event_type: Įvykio tipas (pvz., "AUTHENTICATION", "CHARGING", "FAULT")
            action: Veiksmas (pvz., "AUTH_SUCCESS", "CHARGE_START", "CP_FAULT")
            details: Papildoma informacija (dict arba str)
        """
        if not self.enabled:
            return
        
        timestamp = datetime.now().isoformat()
        
        # Sukurti struktūrizuotą įrašą
        entry = {
            "timestamp": timestamp,           # a. Data ir laikas
            "source_ip": source_ip,           # b. Kas ir iš kur
            "event_type": event_type,         # Kategorija
            "action": action,                 # c. Koks veiksmas
            "details": details or {}          # d. Parametrai
        }
        
        # Įrašyti į failą
        with self.lock:
            try:
                with open(self.log_file, 'a') as f:
                    f.write(json.dumps(entry) + "\n")
            except Exception as e:
                print(f"[AuditLogger] Error writing log: {e}")
    
    def log_authentication(self, source_ip, cp_id, success, reason=None):
        """
        Registruoti autentifikavimo bandymą
        
        Args:
            source_ip: CP IP adresas
            cp_id: CP identifikatorius
            success: True arba False
            reason: Priežastis (jei nesėkminga)
        """
        action = "AUTH_SUCCESS" if success else "AUTH_FAILED"
        details = {
            "cp_id": cp_id,
            "success": success
        }
        
        if not success and reason:
            details["reason"] = reason
        
        self.log_event(source_ip, "AUTHENTICATION", action, details)
    
    def log_charging_session(self, source_ip, cp_id, driver_id, action, kwh=None, amount=None):
        """
        Registruoti įkrovimo sesijos įvykį
        
        Args:
            source_ip: CP arba Driver IP
            cp_id: CP identifikatorius
            driver_id: Vairuotojo ID
            action: "CHARGE_START", "CHARGE_UPDATE", "CHARGE_END"
            kwh: Energija (jei taikoma)
            amount: Suma (jei taikoma)
        """
        details = {
            "cp_id": cp_id,
            "driver_id": driver_id
        }
        
        if kwh is not None:
            details["kwh_delivered"] = kwh
        if amount is not None:
            details["amount_euro"] = amount
        
        self.log_event(source_ip, "CHARGING", action, details)
    
    def log_fault(self, source_ip, cp_id, fault_type, description=None):
        """
        Registruoti gedimą ar klaidą
        
        Args:
            source_ip: CP Monitor IP
            cp_id: CP identifikatorius
            fault_type: "CP_FAULT", "CP_RECOVERY", "CONNECTION_LOST"
            description: Papildomas aprašymas
        """
        details = {
            "cp_id": cp_id,
            "fault_type": fault_type
        }
        
        if description:
            details["description"] = description
        
        self.log_event(source_ip, "SYSTEM_FAULT", fault_type, details)
    
    def log_state_change(self, source_ip, entity_id, old_state, new_state):
        """
        Registruoti būsenos pasikeitimą
        
        Args:
            source_ip: Komponento IP
            entity_id: CP arba Driver ID
            old_state: Sena būsena
            new_state: Nauja būsena
        """
        details = {
            "entity_id": entity_id,
            "old_state": old_state,
            "new_state": new_state
        }
        
        self.log_event(source_ip, "STATE_CHANGE", "STATE_TRANSITION", details)
    
    def log_weather_alert(self, source_ip, cp_id, alert_type, temperature):
        """
        Registruoti orų įspėjimą
        
        Args:
            source_ip: EV_Weather IP
            cp_id: CP identifikatorius
            alert_type: "ALERT_COLD", "WEATHER_OK"
            temperature: Temperatūra
        """
        details = {
            "cp_id": cp_id,
            "alert_type": alert_type,
            "temperature": temperature
        }
        
        self.log_event(source_ip, "WEATHER_ALERT", alert_type, details)
    
    def log_security_event(self, source_ip, event, details=None):
        """
        Registruoti saugumo įvykį
        
        Args:
            source_ip: Šaltinio IP
            event: "KEY_REVOKED", "ENCRYPTION_FAILED", "UNAUTHORIZED_ACCESS"
            details: Papildoma informacija
        """
        self.log_event(source_ip, "SECURITY", event, details)
    
    def get_recent_logs(self, limit=100):
        """
        Gauti paskutinius audito įrašus
        
        Returns:
            List of dicts
        """
        logs = []
        
        try:
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
                
                # Gauti paskutinius N įrašų
                for line in lines[-limit:]:
                    if line.strip():
                        try:
                            logs.append(json.loads(line))
                        except:
                            continue
        
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[AuditLogger] Error reading logs: {e}")
        
        return logs
    
    def search_logs(self, event_type=None, action=None, source_ip=None, start_time=None, end_time=None):
        """
        Ieškoti audito įrašų pagal filtrus
        
        Args:
            event_type: Filtruoti pagal įvykio tipą
            action: Filtruoti pagal veiksmą
            source_ip: Filtruoti pagal IP
            start_time: ISO formatu
            end_time: ISO formatu
        
        Returns:
            Filtered list of logs
        """
        all_logs = self.get_recent_logs(limit=10000)
        filtered = []
        
        for log in all_logs:
            # Tikrinti filtrus
            if event_type and log.get("event_type") != event_type:
                continue
            if action and log.get("action") != action:
                continue
            if source_ip and log.get("source_ip") != source_ip:
                continue
            if start_time and log.get("timestamp") < start_time:
                continue
            if end_time and log.get("timestamp") > end_time:
                continue
            
            filtered.append(log)
        
        return filtered


# ============================================================================
# Globalus audito logger (naudojamas visose sistemose)
# ============================================================================

_audit_logger = None

def get_audit_logger():
    """Gauti globalų audito logger objektą"""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger


# ============================================================================
# Pagalbinės funkcijos (shortcut metodai)
# ============================================================================

def log_auth(ip, cp_id, success, reason=None):
    """Shortcut: log authentication"""
    get_audit_logger().log_authentication(ip, cp_id, success, reason)

def log_charge(ip, cp_id, driver_id, action, kwh=None, amount=None):
    """Shortcut: log charging"""
    get_audit_logger().log_charging_session(ip, cp_id, driver_id, action, kwh, amount)

def log_fault(ip, cp_id, fault_type, desc=None):
    """Shortcut: log fault"""
    get_audit_logger().log_fault(ip, cp_id, fault_type, desc)

def log_state(ip, entity_id, old, new):
    """Shortcut: log state change"""
    get_audit_logger().log_state_change(ip, entity_id, old, new)

def log_weather(ip, cp_id, alert, temp):
    """Shortcut: log weather alert"""
    get_audit_logger().log_weather_alert(ip, cp_id, alert, temp)

def log_security(ip, event, details=None):
    """Shortcut: log security event"""
    get_audit_logger().log_security_event(ip, event, details)