from cryptography.fernet import Fernet
import base64
import hashlib

class EncryptionManager:
    @staticmethod
    def generate_key(password: str) -> bytes:
        """Generate encryption key from password"""
        hashed = hashlib.sha256(password.encode()).digest()
        return base64.urlsafe_b64encode(hashed)
    
    @staticmethod
    def encrypt(message: str, key: bytes) -> str:
        """Encrypt message"""
        f = Fernet(key)
        return f.encrypt(message.encode()).decode()
    
    @staticmethod
    def decrypt(encrypted: str, key: bytes) -> str:
        """Decrypt message"""
        f = Fernet(key)
        return f.decrypt(encrypted.encode()).decode()