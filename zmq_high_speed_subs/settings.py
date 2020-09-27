# -*- coding: utf-8 -*-
import os
import sys
os_env = os.environ

basedir = os.path.abspath(os.path.dirname(__file__))
dir_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class BaseConfig(object):
    APP_DIR = os.path.abspath(os.path.dirname(__file__))  # This directory
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_DIR, os.pardir))
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")

    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

class DevelopmentConfig(BaseConfig):
    """Development configuration."""
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        f'sqlite:///{os.path.join(basedir, "dev.db")}')

class ProductionConfig(BaseConfig):
    """Production configuration."""
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
