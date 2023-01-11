import pandas as pd

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

dates = pd.date_range('2009-07-01', '2010-06-26', freq='5d')

# As documented in https://github.com/pangeo-forge/eNATL60-feedstock/issues/2, this server is not always
# available to service high-bandwidth requests. Concurrency limits in Pangeo Forge, if added as a feature
# in the future, may help. In the interim, re-running this recipe may require checking with the data
# provider regarding the best time to request data from this server.
url_base = (
    'https://ige-meom-opendap.univ-grenoble-alpes.fr'
    '/thredds/fileServer/meomopendap/extract/eNATL60/eNATL60-BLBT02/1d'
)


def make_recipe(var, dep):
    input_url_pattern = (
        url_base + '/eNATL60-BLBT02_y{time:%Y}m{time:%m}d{time:%d}.5d_' + var + dep + '.nc'
    )
    input_urls = [input_url_pattern.format(time=time) for time in dates]
    pattern = pattern_from_file_sequence(input_urls, 'time_counter')
    recipe = XarrayZarrRecipe(pattern, subset_inputs={"time_counter": 2}, target_chunks={'time_counter': 1})
    return recipe


eNATL60_wtides_1d_tsw60m = make_recipe('TSW', '_60m')
