#!/usr/bin/env python
"""
run.py - Ejecutor Principal del Sistema Healthcare Analytics
Uso: python run.py
"""

import sys
import os

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(__file__))

from scripts.main import main


if __name__ == '__main__':
    sys.exit(main())
