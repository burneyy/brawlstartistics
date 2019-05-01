import brawlstatistics as bs

client = bs.start_client()

player = client.get_profile(bs.my_id())
print(player.trophies)  # access attributes using dot.notation
print(player.solo_showdown_victories)  # access using snake_case instead of camelCase<Paste>
