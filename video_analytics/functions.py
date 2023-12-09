# функция для разбиения тегов

import pyspark.sql.functions as F

def split_tags(col):
    return F.split(col, "\\|")