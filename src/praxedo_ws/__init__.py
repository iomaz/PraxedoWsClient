
"""
Package Praxedo_ws
---------

This package provides a SOAP client to connect to the Praxedo web services 
"""
from .soap_client import PraxedoSoapClient
from .ws_utility import get_wo_raw_model

__all__ = ['PraxedoSoapClient', 'get_wo_raw_model']

__version__ = "0.0.1"