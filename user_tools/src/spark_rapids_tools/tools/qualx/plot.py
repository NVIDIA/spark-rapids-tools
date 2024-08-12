# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Plotting functions for QualX model evaluation"""

from functools import reduce
import shap
import numpy as np
import pandas as pd
import xgboost as xgb
# Import optional packages
try:
    import holoviews as hv
except ImportError:
    hv = None
try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
try:
    import seaborn as sns
except ImportError:
    sns = None

hv.extension('bokeh')


def plot_predictions(results, title='prediction vs. actual', x_lim=None, y_lim=None):
    """Plot prediction vs. actual."""
    plt.figure()
    if x_lim:
        plt.xlim(x_lim)
    if y_lim:
        plt.ylim(y_lim)

    # Plot predictions:
    g = sns.regplot(x='y', y='y_pred', data=results)
    g.set(title=title)

    # Line of equality shown in red:
    lims = [0, 1.1 * np.max([results['y'].max(), results['y_pred'].max()])]
    g.plot(lims, lims, '-r')
    return g


def plot_hv_predictions(results):
    # Interactive error plot:
    max_range = results[['y', 'y_pred']].max().max()
    cbar_label = 'pct_error'

    vdims = [
        'y_pred',
        'abs_error',
        'pct_error',
        'appId',
        'appName',
        'sqlID',
        'scaleFactor',
        'Duration',
        'Duration_pred',
    ]
    if 'split' in results.columns:
        vdims.append('split')

    out = hv.Curve([(0, 0), (1.1 * max_range, 1.1 * max_range)]).opts(
        color='red'
    ) * hv.Scatter(results, kdims=['y'], vdims=vdims).opts(
        color=hv.dim(cbar_label),
        cmap='viridis',
        colorbar=True,
        clabel=cbar_label,
        width=1000,
        height=800,
        size=4,
        show_grid=True,
        tools=['hover'],
        toolbar='above',
        xlabel='y',
        ylabel='y_pred',
    )
    return out


def plot_hv_errors(results):
    """Plot histograms of predictions and errors by split."""
    results_ds = hv.Dataset(results)

    pred_plt = results_ds.hist(
        dimension='y', groupby='split', bin_range=(0, 20), bins=100, adjoin=False
    ).opts(hv.opts.Histogram(alpha=0.5, width=600))

    error_plt = results_ds.hist(
        dimension='abs_error',
        groupby=['split'],
        bin_range=(0, 5),
        bins=100,
        adjoin=False,
    ).opts(hv.opts.Histogram(alpha=0.5, width=600))

    return pred_plt + error_plt


def plot_shap(model, features, feature_cols, label_col):
    x_train = features.loc[features['split'] == 'train', feature_cols]
    y_train = features.loc[features['split'] == 'train', label_col]
    d_train = xgb.DMatrix(x_train, y_train)

    explainer = shap.TreeExplainer(model)
    shap_values = explainer(d_train)  # Outputs object

    # SHAP global feature importance:
    shap.summary_plot(shap_values, x_train, plot_type='bar')

    # SHAP violin plots shows phase of SHAP values:
    shap.summary_plot(shap_values, x_train, plot_type='layered_violin')

    shap_values_df = pd.DataFrame(shap_values.values, columns=feature_cols)

    # # Apply filtering:
    # shap_values_df = shap_values_df[shap_values_df['numTasks_sum'].abs() < 1E6]

    # Custom agg of shap values based on sql/non-sql, sqlOp, etc. features.
    shap_global_imp = shap_values_df.abs()
    shap_global_imp = shap_global_imp.mean()

    combine_feature_dict = {
        'cluster+sys': [
            'resourceProfileId',
            'numExecutors',
            'executorCores',
            'maxMem',
            'maxOnHeapMem',
            'maxOffHeapMem',
            'executorMemory',
            'numGpusPerExecutor',
            'executorOffHeap',
            'taskCpu',
            'taskGpu',
        ],
        'sqlOp': [cc for cc in feature_cols if cc.startswith('sqlOp_')],
        'gpuSqlOp': [cc for cc in feature_cols if cc.startswith('xgpu_sqlOp_')],
        'nonSql_metrics': [cc for cc in feature_cols if cc.startswith('nonSql_')],
        'inSqlRatio_metrics': [cc for cc in feature_cols if cc.endswith('Ratio')],
    }

    combine_feature_dict['inSql_metrics'] = [
        cc.replace('nonSql_', '') for cc in combine_feature_dict['nonSql_metrics']
    ]

    combine_labels_vals = reduce(
        lambda x, y: x + y, list(combine_feature_dict.values())
    )
    remaining_cols = list(set(feature_cols) - set(combine_labels_vals))

    # Initialize
    shap_global_imp_combo_attr = shap_global_imp[remaining_cols].copy()

    for k, v in combine_feature_dict.items():
        shap_global_imp_combo_attr[k] = shap_global_imp[v].sum()

    shap_global_imp_combo_attr = shap_global_imp_combo_attr.sort_values(ascending=True)

    shap_global_imp_combo_attr.plot.barh()
