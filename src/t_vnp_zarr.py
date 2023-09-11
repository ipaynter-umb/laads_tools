import t_spinup
import t_pr_hurricanes
import datetime
import numpy as np
import matplotlib as mpl
from matplotlib import pyplot as plt


def date_tick_formatter(tick, tick_loc):

    epoch = datetime.datetime.utcfromtimestamp(0)

    return (epoch + datetime.timedelta(days=int(tick))).strftime('%Y')


def visualize_pr_pixel_hurricanes(dates, ntls):

    year_ticks = []
    curr_year = 2013

    while curr_year < 2024:
        year_ticks.append(datetime.datetime(year=curr_year, month=1, day=1))
        curr_year += 1

    if np.min(ntls) != 65535:

        scaled_pixel_ts = np.multiply(ntls[np.where(ntls != 65535)], 0.1)

        if len(scaled_pixel_ts) > 500:

            window_averages = []
            window_dates = []
            window_size = 7
            window = []

            for date, ntl in zip(dates[np.where(ntls != 65535)], scaled_pixel_ts):
                window.append(ntl)
                if len(window) == window_size:
                    window_averages.append(np.median(window))
                    window_dates.append(date)
                    window = []
            if len(window) > 0:
                window_averages.append(np.median(window))
                window_dates.append(dates[np.where(ntls != 65535)][-1])

            target_percentiles = [10, 20, 30, 40, 50, 60, 70, 80, 90]
            # Get percentiles
            percentiles = np.percentile(scaled_pixel_ts, target_percentiles)

            fig = plt.figure(figsize=(20, 5))
            ax = fig.add_subplot()

            for percentile_ind in [(0, -1), (1, -2), (2, -3), (3, -4)]:
                pc_patch = mpl.patches.Rectangle((dates[0],
                                                  percentiles[percentile_ind[0]]),
                                                 dates[-1] - dates[0],
                                                 percentiles[percentile_ind[1]] - percentiles[
                                                     percentile_ind[0]],
                                                 facecolor='k',
                                                 alpha=0.1)
                ax.add_patch(pc_patch)

            for target_percentile, percentile in zip(target_percentiles, percentiles):
                ax.text(dates[-1] + datetime.timedelta(days=3), percentile, f'- {target_percentile}',
                        va='center')

            ax.text(dates[-1] + datetime.timedelta(days=110), percentiles[4], 'Percentile',
                    ha='center', va='center', rotation='vertical')

            window_h, = ax.plot(window_dates, window_averages, 'k')

            ax.text(datetime.date(year=2022, month=9, day=17), np.max(scaled_pixel_ts) + 0.1, 'Fiona',
                    ha='center')

            for hurricane_date in t_pr_hurricanes.get_hurricane_dates():
                hurr_h, = ax.plot([hurricane_date, hurricane_date], [0, np.max(scaled_pixel_ts)], 'r--')

            ntl_h = ax.scatter(dates[np.where(ntls != 65535)], scaled_pixel_ts, marker='.')

            ax.set_ylim(0, np.max(scaled_pixel_ts) + (np.max(scaled_pixel_ts) * 0.1))
            ax.set_xlim(dates[0], dates[-1])
            ax.set_ylabel('Night Time Lights (nW cm$^-$$^2$ sr$^-$$^1$)')
            ax.set_xlabel('Date')
            ax.set_xticks(year_ticks)
            ax.xaxis.set_major_formatter(plt.FuncFormatter(date_tick_formatter))
            ax.legend(labels=['Night Time Lights', 'Rolling Median', 'Hurricanes,\nTropical Storms'],
                      handles=[ntl_h, window_h, hurr_h],
                      edgecolor='k',
                      bbox_to_anchor=(1.01, 1))

            plt.show()


def visualize_pr_pixel_angles(ntls, az_angles, zen_angles):

    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(1, 2, 1)

    ax.scatter(np.multiply(az_angles[np.where(np.all([ntls != 65535, az_angles != -32768], axis=0))], 0.01),
               np.multiply(ntls[np.where(np.all([ntls != 65535, az_angles != -32768], axis=0))], 0.1),
               marker='.')

    ax.set_xlabel('Angle (degrees)')

    ax = fig.add_subplot(1, 2, 2)

    ax.scatter(np.multiply(zen_angles[np.where(np.all([ntls != 65535, zen_angles != -32768], axis=0))], 0.01),
               np.multiply(ntls[np.where(np.all([ntls != 65535, zen_angles != -32768], axis=0))], 0.1),
               marker='.')

    ax.set_xlabel('Angle (degrees)')

    plt.show()
