import numpy as np
import pandas as pd
import matplotlib; matplotlib.use('Agg') # don't fail when on headless server
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession, types


colour_schema = types.StructType([
    types.StructField('R', types.IntegerType(), False),
    types.StructField('G', types.IntegerType(), False),
    types.StructField('B', types.IntegerType(), False),
    types.StructField('word', types.StringType(), False),
    types.StructField('confidence', types.StringType(), False),
])


def rgb2lab_query(table_name='__THIS__', passthrough_columns=None, input_bytes=True,
                  r='R', g='G', b='B', out_l='labL', out_a='labA', out_b='labB'):
    """
    Build SQL query to convert RGB colours to LAB colours.

    table_name: name of the input table to query from. Must be '__THIS__' if being used in a SQLTransformer.
    passthrough_columns: list of column names that should be preserved and selected into the resulting table.
    input_bytes: if True, assumes RGB inputs are integers 0-255. If not, assumes floats 0-1.
    r, g, b, out_l, out_a, out_b: the input and output column names.
    
    Based on the calculations in scikit image for rgb2xyz and xyz2lab (with illuminant="D65", observer="2")
    https://github.com/scikit-image/scikit-image/blob/master/skimage/color/colorconv.py
    """
    passthrough = [r, g, b]
    if passthrough_columns:
        passthrough.extend(passthrough_columns)
    passthrough = ', '.join(passthrough)

    r2x_op = """CASE WHEN {incol} > 0.04045 THEN POWER(({incol} + 0.055) / 1.055, 2.4) ELSE {incol} / 12.92 END"""
    x2l_op = """CASE WHEN {incol} > 0.008856 THEN POWER({incol}, 1/3) ELSE (7.787 * {incol} + 16./116) END"""

    if input_bytes:
        rgb_query = """SELECT {r}/255 as r1, {g}/255 as g1, {b}/255 as b1, {passthrough} FROM {table_name}"""
    else:
        rgb_query = """SELECT {r} as r1, {g} as g1, {b} as b1, {passthrough} FROM {table_name}"""

    query = """WITH
        real_rgb AS (
            {rgb_query}
        ),
        to_xyz_1 AS (
            SELECT
                {op1_r} AS r2,
                {op1_g} AS g2,
                {op1_b} AS b2,
                {passthrough}
            FROM real_rgb
        ),
        to_xyz_2 AS (
            SELECT
                (r2*0.412453 + g2*0.357580 + b2*0.180423)/0.95047 AS x1,
                (r2*0.212671 + g2*0.715160 + b2*0.072169)         AS y1,
                (r2*0.019334 + g2*0.119193 + b2*0.950227)/1.08883 AS z1,
                {passthrough}
            FROM to_xyz_1
        ),
        to_lab_1 AS (
            SELECT
                {op2_x} AS x2,
                {op2_y} AS y2,
                {op2_z} AS z2,
                {passthrough}
            FROM to_xyz_2
        ),
        to_lab_2 AS (
            SELECT
                116*y2 - 16 AS l1,
                500*(x2 - y2) AS a1,
                200*(y2 - z2) AS b1,
                {passthrough}
            FROM to_lab_1
        )
        SELECT {passthrough}, l1 as {out_l}, a1 as {out_a}, b1 as {out_b} FROM to_lab_2
    """.format(
        rgb_query=rgb_query.format(r=r, g=g, b=b, table_name=table_name, passthrough=passthrough),
        table_name=table_name, passthrough=passthrough,
        op1_r=r2x_op.format(incol='r1'), op1_g=r2x_op.format(incol='g1'), op1_b=r2x_op.format(incol='b1'),
        op2_x=x2l_op.format(incol='x1'), op2_y=x2l_op.format(incol='y1'), op2_z=x2l_op.format(incol='z1'),
        out_l=out_l, out_a=out_a, out_b=out_b
    )
    return query


def rgb2hsv_query(table_name='__THIS__', passthrough_columns=None, input_bytes=True,
                  r='R', g='G', b='B', h='H', s='S', v='V'):
    """
    Build SQL query to convert RGB colours to HSV colours.

    table_name: name of the input table to query from. Must be '__THIS__' if being used in a SQLTransformer.
    passthrough_columns: list of column names that should be preserved and selected into the resulting table.
    input_bytes: if True, assumes RGB inputs are integers 0-255. If not, assumes floats 0-1.
    r, g, b, h, s, v: the input and output column names.
    
    Based on the calculations in scikit image for rgb2hsv:
    https://github.com/scikit-image/scikit-image/blob/master/skimage/color/colorconv.py
    
    With input_bytes true, this generates a query too large for the Spark SQL optimizer. It should still
    work, but generates a disturbing error message along the way.
    """
    passthrough = [r, g, b]
    if passthrough_columns:
        passthrough.extend(passthrough_columns)
    passthrough = ', '.join(passthrough)

    if input_bytes:
        rgb_query = """SELECT {r}/255 as r1, {g}/255 as g1, {b}/255 as b1, {passthrough} FROM {table_name}"""
    else:
        rgb_query = """SELECT {r} as r1, {g} as g1, {b} as b1, {passthrough} FROM {table_name}"""

    query = """WITH
        real_rgb AS (
            {rgb_query}
        ),
        min_max AS (
            SELECT
                CASE WHEN r1 > g1 AND r1 > b1
                    THEN r1
                    ELSE (CASE WHEN g1 > b1 THEN g1 ELSE b1 END)
                END AS rgb_max,
                CASE WHEN r1 < g1 AND r1 < b1
                    THEN r1
                    ELSE (CASE WHEN g1 < b1 THEN g1 ELSE b1 END)
                END AS rgb_min,
                r1, g1, b1,
                {passthrough}
            FROM real_rgb
        ),
        deltas AS (
            SELECT
                rgb_max,
                rgb_max - rgb_min AS delta,
                r1, g1, b1,
                {passthrough}
            FROM min_max
        ),
        hsvs AS (
            SELECT
                (CASE WHEN delta=0 THEN 0 ELSE
                    CASE WHEN r1 > g1 AND r1 > b1
                    THEN (g1 - b1) / delta
                    ELSE (CASE WHEN g1 > b1
                        THEN 2 + (b1 - r1) / delta
                        ELSE 4 + (r1 - g1) / delta END)
                END END) / 6 AS h,
                CASE WHEN delta=0 THEN 0 ELSE delta/rgb_max END AS s,
                rgb_max AS v,
                {passthrough}
            FROM deltas
        )
        SELECT {passthrough}, CASE WHEN h<0 THEN h+1 ELSE h END AS {h}, s AS {s}, v AS {v} FROM hsvs
    """.format(
        rgb_query=rgb_query.format(r=r, g=g, b=b, table_name=table_name, passthrough=passthrough),
        table_name=table_name, passthrough=passthrough,
        h=h, s=s, v=v
    )
    return query


# representative RGB colours for each label, for prediction display
COLOUR_RGB = {
    'red': (255, 0, 0),
    'orange': (255, 114, 0),
    'yellow': (255, 255, 0),
    'green': (0, 230, 0),
    'blue': (0, 0, 255),
    'purple': (187, 0, 187),
    'brown': (117, 60, 0),
    'pink': (255, 187, 187),
    'black': (0, 0, 0),
    'grey': (150, 150, 150),
    'white': (255, 255, 255),
}
_name_to_rgb = np.vectorize(COLOUR_RGB.get, otypes=[np.uint8, np.uint8, np.uint8])


def _rgb_grid(labelCol='word', lum=70, resolution=256, r='R', g='G', b='B'):
    """
    Create a slice of LAB colour space at the given luminosity, converted to 0-255 RGB colours, in a Spark DataFrame.
    """
    from skimage.color import lab2rgb
    spark = SparkSession.builder.getOrCreate()

    wid = resolution
    hei = resolution

    # create a hei*wid grid of LAB colour values, with L=lum
    ag = np.linspace(-100, 100, wid)
    bg = np.linspace(-100, 100, hei)
    aa, bb = np.meshgrid(ag, bg)
    ll = lum * np.ones((hei, wid))
    lab_grid = np.stack([ll, aa, bb], axis=2)

    # convert to RGB
    rgb = lab2rgb(lab_grid).reshape(-1, 3)
    rgb_pd = pd.DataFrame()
    rgb_pd[r] = rgb[:, 0] * 255
    rgb_pd[g] = rgb[:, 1] * 255
    rgb_pd[b] = rgb[:, 2] * 255
    rgb_pd[labelCol] = 'black' # fill in fake predictions, so pipeline will accept this DF
    return spark.createDataFrame(rgb_pd)


def _label_dict(model):
    """
    Build a dictionary of index to labels from the given StringIndexerModel or PipelineModel.
    """
    from pyspark.ml.feature import StringIndexerModel
    from pyspark.ml import PipelineModel
    
    # find the StringIndexerModel in the pipeline so we can reconstruct colour names
    if isinstance(model, PipelineModel):
        indexermodel = [m for m in model.stages if isinstance(m, StringIndexerModel)][0]
    elif isinstance(model, StringIndexerModel):
        indexermodel = model
    else:
        raise TypeError('Unknown type of model argument: must be StringIndexerModel or PipelineModel'
            '(with one StringIndexerModel).')

    # build dict of index -> label
    labels = indexermodel.labels
    return dict((float(index), label) for index, label in zip(range(len(labels)), labels))


def plot_predictions(model, description, lum=70, resolution=256, output_filename=None,
                     r='R', g='G', b='B', labelCol='word', predictionCol='prediction'):
    """
    Create a slice of LAB colour space with given luminance; predict with the model; plot the results.
    """
    wid = resolution
    hei = resolution
    n_ticks = 5
    if output_filename is None:
        output_filename = 'predictions-%s.png' % (description,)

    # use the model to make some predictions on the colours we want to display
    rgb = _rgb_grid(labelCol=labelCol, lum=lum, resolution=resolution)
    predictions = model.transform(rgb).cache()

    # inspect the model to figure out label number -> word mapping
    labeldict = _label_dict(model)

    # extract RGB values
    rgb = predictions.select(predictions[r], predictions[g], predictions[b]).toPandas().values / 255
    rgb_pixels = rgb.reshape((hei, wid, 3))

    # convert predictions to RGB colours so we can plot
    predictions = predictions.select(predictions[predictionCol]).toPandas()[predictionCol]
    pixels = np.stack(_name_to_rgb(predictions.apply(labeldict.get)), axis=1) / 255
    pixels = pixels.reshape((hei, wid, 3))

    # plot input and predictions
    plt.figure(figsize=(10, 5))
    plt.suptitle('Predictions at luminance=%g for %s' % (lum, description))
    plt.subplot(1, 2, 1)
    plt.title('Inputs')
    plt.xticks(np.linspace(0, wid, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.yticks(np.linspace(0, hei, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.xlabel('A')
    plt.ylabel('B')
    plt.imshow(rgb_pixels)

    plt.subplot(1, 2, 2)
    plt.title('Predicted Labels')
    plt.xticks(np.linspace(0, wid, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.yticks(np.linspace(0, hei, n_ticks), np.linspace(-100, 100, n_ticks))
    plt.xlabel('A')
    plt.imshow(pixels)
    
    plt.savefig(output_filename)
