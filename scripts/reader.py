import os
from pathlib import Path
import csv
import sys

def fil_reader(src_fil_dir):
    with open(src_fil_dir,'r') as file:
        for record in file:
            yield record