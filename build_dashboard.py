import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Line, Bar, Page
from pyecharts.components import Table
from pyecharts.globals import ThemeType

# 1. 读取数据
gmv_df = pd.read_csv("daily_gmv.csv", header=None, names=["dt", "gmv"])
order_df = pd.read_csv("daily_order_cnt.csv", header=None, names=["dt", "order_cnt"])
user_df = pd.read_csv("daily_user_cnt.csv", header=None, names=["dt", "user_cnt"])
refund_df = pd.read_csv("daily_refund.csv", header=None, names=["dt", "total_refund"])
cat_df = pd.read_csv("top_category_gmv.csv", header=None, names=["category", "gmv"])
shop_df = pd.read_csv("top_shop_gmv.csv", header=None, names=["shop_id", "shop_type", "gmv"])

# 2. 类型处理
for df, col in [
    (gmv_df, "gmv"),
    (order_df, "order_cnt"),
    (user_df, "user_cnt"),
    (refund_df, "total_refund"),
    (cat_df, "gmv"),
    (shop_df, "gmv"),
]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

shop_df["shop_label"] = shop_df["shop_id"].astype(str) + " (" + shop_df["shop_type"].astype(str) + ")"

# 3. 核心指标
latest_gmv = float(gmv_df.iloc[-1]["gmv"])
latest_order_cnt = int(order_df.iloc[-1]["order_cnt"])
latest_user_cnt = int(user_df.iloc[-1]["user_cnt"])
latest_refund = float(refund_df.iloc[-1]["total_refund"])
avg_gmv = float(gmv_df["gmv"].mean())
max_gmv = float(gmv_df["gmv"].max())

metric_table = Table()
headers = ["Metric", "Value"]
rows = [
    ["Latest Day GMV", f"{latest_gmv:,.2f}"],
    ["Latest Day Order Count", f"{latest_order_cnt:,}"],
    ["Latest Day User Count", f"{latest_user_cnt:,}"],
    ["Latest Day Refund Amount", f"{latest_refund:,.2f}"],
    ["Average Daily GMV", f"{avg_gmv:,.2f}"],
    ["Max Daily GMV", f"{max_gmv:,.2f}"],
]
metric_table.add(headers, rows)
metric_table.set_global_opts(
    title_opts=opts.ComponentTitleOpts(
        title="E-Commerce Data Warehouse Dashboard",
        subtitle="Core Metrics Overview"
    )
)

def build_line(df, x_col, y_col, title, y_name):
    return (
        Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1400px", height="500px"))
        .add_xaxis(df[x_col].tolist())
        .add_yaxis(
            y_name,
            df[y_col].round(2).tolist() if df[y_col].dtype.kind in "fc" else df[y_col].tolist(),
            is_smooth=True,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
            yaxis_opts=opts.AxisOpts(name=y_name),
            datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
        )
    )

gmv_line = build_line(gmv_df, "dt", "gmv", "Daily GMV Trend", "GMV")
order_line = build_line(order_df, "dt", "order_cnt", "Daily Order Count Trend", "Order Count")
user_line = build_line(user_df, "dt", "user_cnt", "Daily User Count Trend", "User Count")
refund_line = build_line(refund_df, "dt", "total_refund", "Daily Refund Amount Trend", "Refund Amount")

cat_df = cat_df.sort_values("gmv", ascending=True)
category_bar = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1400px", height="500px"))
    .add_xaxis(cat_df["category"].tolist())
    .add_yaxis("GMV", cat_df["gmv"].round(2).tolist(), label_opts=opts.LabelOpts(is_show=False))
    .reversal_axis()
    .set_global_opts(
        title_opts=opts.TitleOpts(title="Top Categories by GMV"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(name="GMV"),
        yaxis_opts=opts.AxisOpts(name="Category"),
    )
)

shop_df = shop_df.sort_values("gmv", ascending=True)
shop_bar = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1400px", height="500px"))
    .add_xaxis(shop_df["shop_label"].tolist())
    .add_yaxis("GMV", shop_df["gmv"].round(2).tolist(), label_opts=opts.LabelOpts(is_show=False))
    .reversal_axis()
    .set_global_opts(
        title_opts=opts.TitleOpts(title="Top Shops by GMV"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(name="GMV"),
        yaxis_opts=opts.AxisOpts(name="Shop"),
    )
)

page = Page(page_title="E-Commerce DW Dashboard", layout=Page.SimplePageLayout)
page.add(
    metric_table,
    gmv_line,
    order_line,
    user_line,
    refund_line,
    category_bar,
    shop_bar,
)
page.render("dashboard.html")

print("dashboard.html 已生成")