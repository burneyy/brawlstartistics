#!/usr/bin/env python
import brawlstartistics as bs
import pandas as pd

print("Reading in brawlers..")
df = bs.read_brawlers()
print(f"Total number of brawlers: {len(df)}")

meanTrophies = df.groupby(["name", "power"])["trophies"].mean().rename("meanTrophies")
semTrophies = df.groupby(["name", "power"])["trophies"].sem().rename("semTrophies")

resdf = pd.concat([meanTrophies, semTrophies], axis=1)

print(resdf.query('name == "Carl"'))

