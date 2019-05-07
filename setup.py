from setuptools import setup

setup(
    name='brawlstartistics',
    version='1.0',
    author='Daniel Berninghoff',
    author_email='daniel.berninghoff@gmail.com',
    packages=[
        "brawlstartistics",
        "brawlstartistics.scripts"
    ],
    install_requires=[
        'pandas'
    ],
    entry_points={
        "console_scripts" : [
            "bs_crawl = brawlstartistics.scripts.crawl:main"
        ]
    }
)


