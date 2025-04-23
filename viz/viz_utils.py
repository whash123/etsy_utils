import matplotlib.pyplot as plt
import seaborn as sns
from collections.abc import Iterable
import pandas.io.formats.style


class EtsyColors():
    """
    Instantiate an object with a `library` property containing Etsy's brand colors.
    See: https://drive.google.com/file/d/185hhTOMsBWTacLE8fZo44czjJloB1qCb/view
    """


    def __init__(self):
        self.library = {
            'orange': {'dark': '#CF4018', 'medium': '#F1641E', 'light': '#FAA077'},
            'denim': {'dark': '#232347', 'medium': '#2F466C', 'light': '#4D6BC6'},
            'grey': {'dark': '#222222', 'medium': '#595959', 'light': '#757575'},
            'lavender': {'dark': '#3E1C53', 'medium': '#654B77', 'light': '#9560B8'},
            'beeswax': {'dark': '#A66800', 'medium': '#FAA129', 'light': '#FDD95C'},
            'slime': {'dark': '#1C4A21', 'medium': '#258635', 'light': '#9EC063'},
            'brick': {'dark': '#540D17', 'medium': '#A61A2E', 'light': '#FD9184'},               
            'turquoise': {'dark': '#1A3B38', 'medium': '#2F766D', 'light': '#7ED4BD'},
            'bubblegum': {'dark': '#592642', 'medium': '#B54C82', 'light': '#F592B8'}
        }


    def __hex_fetcher(self, hue=None, tint=None):
        # If the input object isn't already a non-string iterable, make it one
        def to_iter(x):
            return [x] if not isinstance(x, Iterable) or isinstance(x, str) else x
        
        # Cast all str items in the input object to lowercase
        def parse_str(x):
            return [i.lower() if isinstance(i, str) else i for i in x]

        def parse_hue(hue):
            hue = parse_str(to_iter(hue))
            
            all_hues = self.library.keys()
            core_hues = ['orange', 'denim', 'grey']

            if any([i in (None, 'all') for i in hue]):
                hue = all_hues
            elif any([i == 'core' for i in hue]):
                hue = core_hues
            elif any([i == 'extended' for i in hue]):
                hue = [i for i in all_hues if i not in core_hues]
            else:
                hue = hue
            
            return hue

        def parse_tint(tint):
            tint = parse_str(to_iter(tint))

            all_tints = ['dark', 'medium', 'light']

            if any([i in (None, 'all') for i in tint]):
                tint = all_tints
            else:
                tint = tint
            
            return tint
        
        hue, tint = parse_hue(hue), parse_tint(tint)

        hexes = []
        for h in hue:
            for t in tint:
                try:
                    hexes.append(self.library.get(h).get(t))
                except:
                    pass
        
        return hexes


    def make_palette(self, hue=None, tint=None, n_colors=None):
        """
        Generate a custom color palette out of the options in `self.library`.
        """

        hexes = self.__hex_fetcher(hue, tint)

        pal = sns.color_palette(hexes, n_colors=n_colors)
        
        self.palette = pal if len(pal) > 0 else None

    
    def plot_palette(self):
        """
        Plot the colors in the `self.palette` property if it exists; return `None` otherwise.
        """

        try:
            pal = self.palette
        except:
            pal = None

        if pal:
            sns.palplot(pal)
            plt.show()
        else:
            return None


    def plot_library(self):
        """
        Plot the full library of Etsy colors in the `self.library` property.
        """
        pal = self.__hex_fetcher()
        sns.palplot(pal)
        plt.show()

class QStyler(pd.io.formats.style.Styler):
    emap = {'cell': 'td',
            'cells': 'td',
            'data': 'td',
            'table-header': 'th',
            'row-header': 'th.row_heading.level1',
            'row-label': 'th.row_heading.level0',
            'col-header': 'th.col_heading.level1',
            'col-label': 'th.col_heading.level0'}

    @property
    def _constructor(self):
        return self.style


    def format_cell_values(self, formatter, subset=None, inplace=False):
        s = self.format(formatter, subset=subset)

        if inplace:
            self = s
        else:
            return s


    def _style_setter(self, mapper, prop_name, inplace):
        s = self
        for element in mapper.keys():
            selector = self.emap.get(element)
            prop_val = mapper.get(element)
            s.set_table_styles([{'selector': selector, 'props': [(prop_name, prop_val)]}], overwrite=False)

        if inplace:
            self = s
        else:
            return s


    def set_background_color(self, mapper, inplace=False):
        return self._style_setter(mapper, 'background-color', inplace)


    def set_text_color(self, mapper, inplace=False):
        return self._style_setter(mapper, 'color', inplace)


    def set_text_size(self, mapper='11px', inplace=False):
        if isinstance(mapper, str):
            mapper = dict.fromkeys(self.emap, mapper)
        else:
            pass
        return self._style_setter(mapper, 'font-size', inplace)


    def set_text_align(self, mapper='right', inplace=False):
        if isinstance(mapper, str):
            mapper = dict.fromkeys(self.emap, mapper)
        else:
            pass
        return self._style_setter(mapper, 'text-align', inplace)


    def set_font_style(self, mapper, inplace=False):
        return self._style_setter(mapper, 'font-style', inplace)


    def set_font_family(self, mapper='Arial', inplace=False):
        if isinstance(mapper, str):
            mapper = dict.fromkeys(self.emap, mapper)
        else:
            pass
        return self._style_setter(mapper, 'font-family', inplace)


    def set_column_width(self, px=100, inplace=False):
        s = self.set_table_styles([dict(selector='th.col_heading', props=f'width: {px}px')], overwrite=False)

        if inplace:
            self = s
        else:
            return s


    def set_border(self, mapper, which=None, inplace=False):
        border = 'border' + f'-{which}' if which else 'border'
        return self._style_setter(mapper, 'border', inplace)


    def set_background_gradient(self, color='goldenrod', subset=None, inplace=False):
        cmap = sns.light_palette(color, as_cmap=True)
        s = self.background_gradient(cmap, axis=None, subset=subset)

        if inplace:
            self = s
        else:
            return s