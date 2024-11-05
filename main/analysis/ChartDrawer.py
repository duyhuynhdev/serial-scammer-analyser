import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.colors as mcolors
from wordcloud import WordCloud, STOPWORDS


def bar(x, y, title=None, x_label=None, y_label=None, legend_label=None, show_bar=True):
    fig, ax = plt.subplots()
    p = ax.bar(x, y)
    if title is not None:
        ax.set_title(title)
    if show_bar is not None:
        ax.bar_label(p)
    if y_label is not None:
        ax.set_ylabel(y_label)
    if x_label is not None:
        ax.set_xlabel(x_label)
    if legend_label is not None:
        ax.legend(title=legend_label)
    plt.show()


def hist(x, bins, title=None, x_label=None, y_label=None):
    # plt.style.use('_mpl-gallery')
    fig, ax = plt.subplots()
    ax.hist(x, bins=bins)
    if title is not None:
        ax.set_title(title)
    if y_label is not None:
        ax.set_ylabel(y_label)
    if x_label is not None:
        ax.set_xlabel(x_label)
    plt.show()


def hbar(x, y, title=None, x_label=None, y_label=None, legend_label=None):
    fig, ax = plt.subplots()
    y_pos = np.arange(len(x))
    p = ax.barh(x, y)
    ax.barh(y_pos, y, align='center')
    ax.set_yticks(y_pos, labels=x)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.bar_label(p)
    if title is not None:
        ax.set_title(title)
    if y_label is not None:
        ax.set_ylabel(y_label)
    if x_label is not None:
        ax.set_xlabel(x_label)
    if legend_label is not None:
        ax.legend(title=legend_label)
    plt.show()


def multi_bars(x, y, title=None, x_label=None, y_label=None, legend_label=None, show_bar=True, bar_width=0.25, tick_alignment=0.25):
    fig, ax = plt.subplots(layout='constrained')
    x_len = np.arange(len(x))
    multiplier = 0
    for key, value in y.items():
        offset = bar_width * multiplier
        rects = ax.bar(x_len + offset, value, bar_width, label=key)
        if show_bar:
            ax.bar_label(rects, padding=len(y))
        multiplier += 1
    ax.set_xticks(x_len + tick_alignment, x)
    if title is not None:
        ax.set_title(title)
    if y_label is not None:
        ax.set_ylabel(y_label)
    if x_label is not None:
        ax.set_xlabel(x_label)
    if legend_label is not None:
        ax.legend(title=legend_label, loc='upper left')
    plt.show()


def pie(label, data, title=None, legend_label=None, solve_overlapping=False):
    fig, ax = plt.subplots(figsize=(6, 3), subplot_kw=dict(aspect="equal"))

    if solve_overlapping:
        def func(label, value, allvals):
            percent = np.round(value * 100. / np.sum(allvals), 2)
            # return label + f": {percent:.2f}%({value:d})"
            return label + f": {percent:.2f}%"

        wedges, texts = ax.pie(data, wedgeprops=dict(width=0.5), startangle=-40)
        annotations = [func(label[i], data[i], data) for i in range(0, len(label))]
        ax.legend(wedges, annotations,
                  title=legend_label,
                  loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1))
    else:
        def func(pct, allvals):
            absolute = int(np.round(pct / 100. * np.sum(allvals)))
            return f"{pct:.1f}%\n({absolute:d})"

        wedges, texts, autotexts = ax.pie(data, autopct=lambda pct: func(pct, data),
                                          textprops=dict(color="w"))

        ax.legend(wedges, label,
                  title=legend_label,
                  loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1))

        plt.setp(autotexts, size=8, weight="bold")
    if title is not None:
        ax.set_title(title)

    plt.show()


def pie_2(label, data, title):
    fig, ax = plt.subplots(figsize=(6, 3), subplot_kw=dict(aspect="equal"))

    def func(label, value, allvals):
        percent = int(np.round(value * 100. / np.sum(allvals)))
        return label + f": {percent:.1f}%({value:d})"

    wedges, texts = ax.pie(data, wedgeprops=dict(width=0.5), startangle=-40)

    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-"),
              bbox=bbox_props, zorder=0, va="center")

    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1) / 2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = f"angle,angleA=0,angleB={ang}"
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        print(i)
        print(label[i])
        print(data[i])
        a = func(label[i], data[i], data)
        ax.annotate(a, xy=(x, y), xytext=(1.35 * np.sign(x), 1.4 * y),
                    horizontalalignment=horizontalalignment, **kw)

    ax.set_title(title)

    plt.show()


def wordcloud(text: str, title):
    fig, ax = plt.subplots(figsize=(4, 4), facecolor=None)
    stopwords = set(STOPWORDS)
    wordcloud = WordCloud(width=800,
                          height=800,
                          background_color='white',
                          stopwords=stopwords,
                          min_font_size=10).generate(text)
    # plot the WordCloud image
    print(wordcloud.words_.keys())
    plt.imshow(wordcloud)
    plt.axis("off")
    ax.set_title(title)
    plt.tight_layout(pad=0)
    plt.show()


class MultiLineChart:
    color_set = {
        "ETH" : [mcolors.CSS4_COLORS["dodgerblue"],mcolors.CSS4_COLORS["darkcyan"],mcolors.CSS4_COLORS["blueviolet"],mcolors.CSS4_COLORS["springgreen"]],
        "BNB" : [mcolors.CSS4_COLORS["darkorange"],mcolors.CSS4_COLORS["gold"],mcolors.CSS4_COLORS["tomato"],mcolors.CSS4_COLORS["chocolate"]]
    }
    def __init__(self, x, ys, labels, title, x_label=None, y_label=None, legend_label=None, colors=None, legend_pos = 'upper left'):
        self.x = x
        self.ys = ys
        self.labels = labels
        self.title = title
        self.x_label = x_label
        self.y_label = y_label
        self.legend_label = legend_label
        self.legend_pos = legend_pos
        self.colors = colors if colors is not None else list(mcolors.TABLEAU_COLORS.values())


def multi_lines_plot(ax, setting: MultiLineChart):
    line_styles = ["-", "--", "-.", ":"]
    for i in range(len(setting.ys)):
        y = setting.ys[i]
        label = setting.labels[i]
        ls = line_styles[i % len(line_styles)]
        color = setting.colors[i % len(setting.colors)]
        ax.plot(setting.x, y, label=label, linestyle=ls, color=color)
    if setting.title is not None:
        ax.set_title(setting.title)
    if setting.y_label is not None:
        ax.set_ylabel(setting.y_label)
    if setting.x_label is not None:
        ax.set_xlabel(setting.x_label)
    if setting.legend_label is not None:
        ax.legend(title=setting.legend_label, loc=setting.legend_pos)

def pair_chart_multi_lines_plot(setting1: MultiLineChart, setting2: MultiLineChart):
    fig, ax = plt.subplots(1,2)
    multi_lines_plot(ax[0], setting1)
    multi_lines_plot(ax[1], setting2)
    plt.show()

def multi_lines(filename, setting1: MultiLineChart):
    fig, ax = plt.subplots()
    fig.set_figwidth(5)
    fig.set_figheight(3)
    multi_lines_plot(ax, setting1)
    fig.tight_layout()
    plt.savefig(filename, dpi=100)
    plt.show()

if __name__ == '__main__':
    # x = ['apple', 'blueberry', 'cherry', 'orange']
    # y = [40, 100, 30, 55]
    # bar(x, y, "My chart", "Year", "Value")
    #
    # x = ("Adelie", "Chinstrap", "Gentoo")
    # y = {
    #     'Bill Depth': (18.35, 18.43, 14.98),
    #     'Bill Length': (38.79, 48.83, 47.50),
    #     'Flipper Length': (189.95, 195.82, 217.19),
    # }
    # multi_bars(x, y, "", "", "", "", False, 0.25, 0.25)
    # x = ['apple', 'blueberry', 'cherry', 'orange']
    # y = [40, 100, 30, 55]
    # hbar(x, y, "My chart", "Year", "Value")

    # label = ['WETH', 'USDC', 'USDT', 'DAI', 'UNI', 'others']
    # value = [33295, 426, 152, 52, 25, 331]
    #
    # pie(label, value, "Top Valuable Tokens", "Token", True)

    # sample = "Lorem ipsum dolor sit amet, nonumy dictas disputando et vel, ad eam iudico utamur accommodare, ius no vero sanctus. Eum ne zril omnes appellantur, posse fabulas tractatos cum ei, eam ea dicat integre argumentum. Cu has diam graece impedit. Illud alterum epicuri in vel, at pri agam contentiones, ea cum aeterno suscipiantur comprehensam. Cum te melius ullamcorper, mel graece pertinacia te. Per labore perfecto ut, populo quodsi epicuri mei ex, eu mea justo cetero consequuntur."
    # wordcloud(sample, "Token")
    x = [0.985294, 0.572660, 0.359454, 0.427184, 0.599865, 0.800000, 1.000000, 0.370925, 0.827182, 0.263624]
    hist(x, bins=100)
