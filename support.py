# Pull In from CSV

from csv import reader
from os import walk


def import_csv_layout(path):
    terrain_map = []
    with open(path) as level_map:
        layout = reader(level_map, delimiter=",")
        for row in layout:
            terrain_map.append(list(row))
        return terrain_map


def import_folder(path):
    for _, __, img_files in walk(path):
        for image in img_files:
            full_path = path + '/' + image


import_folder('../graphics/Grass')
# combine the grass path with the .png grass 1-3 to get filepath to image and make a surf
#1:43:00
