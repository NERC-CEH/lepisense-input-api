from enum import Enum


class Role(str, Enum):
    READ = 'read'
    WRITE = 'write'
    ADMIN = 'admin'
    ROOT = 'root'
