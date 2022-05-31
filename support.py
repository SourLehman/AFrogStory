# Pull In from CSV

from csv import reader


def import_csv_layout(path):
    with open(path) as level_map:
        layout = reader(level_map, delimiter=",")
        for row in layout:
            print(row)


import_csv_layout('Levels/level_data/tree_layer_Tile_Layer_3.csv')
