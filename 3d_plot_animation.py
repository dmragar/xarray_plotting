import dask
import datetime
import matplotlib.pyplot as plt
import xarray as xr
import scipy.ndimage
from snobedo.lib.dask_utils import *
import matplotlib as mpl
mpl.rcParams['figure.dpi'] = 300
#plt.style.use(['science','notebook'])
from pathlib import PurePath, Path
import numpy as np
from dask.distributed import Client
from dask import delayed


def main():
    """ 
    save animation figs in parallel with Dask, using CHPC resources
    """
    with run_with_client(6, 32) as client:

        snow_ds = load_dataset()
        dem = load_dem()
        
        # scatter to prevent transfering large arrays to client workers 
        future_snow = client.scatter(snow_ds)
        future_dem = client.scatter(dem)
        
        print('datasets loaded and scattered')
        
        dask_queue = []
    
        for day_int in range(len(snow_ds['thickness'])):
            dask_queue.append(
                dask.delayed(plot_both)(future_snow, day_int, future_dem)
            )
        dask.compute(params)
        

def load_dataset():
    """
    load iSnobal model run and select for hour 22
    """
    snow = xr.open_mfdataset(
        f'{Path.home()}/skiles_storage/' \
        'AD_isnobal/animas_direct_update/wy2020/crb/run*/*snow.nc',
    #data_vars='thickness',
    parallel=True,
    ).sel(time=datetime.time(22))
    return snow


def load_dem():
    """
    load dem and then smooth terrain for plotting
    """
    dem = xr.open_dataset("/uufs/chpc.utah.edu/common/home/u1321700/basin_setup_animasdolores/20211009_sj/topo.nc")
    gauss = scipy.ndimage.gaussian_filter(dem['dem'], sigma=10)
    dem['elevation (m)'] = (('y', 'x'), gauss)
    return dem


def get_colors(inp, colormap, vmin=0, vmax=3):
    norm = plt.Normalize(vmin, vmax)
    return colormap(norm(inp))


def plot_both(snow_ds, day_int, dem):
    """
    creates plot with two panels showing 3d view of model output, 
    as well as labels at given locations, in this case SASP. 
    """
    # do no show plot output, only save
    mpl.use("agg")
    # assign angle to day here for clarity
    view_angle = day_int

    fig = plt.figure(figsize=(11,6))
    # First subplot -----------------------------
    ax = fig.add_subplot(1, 2, 1)
    snow_ds['thickness'][day_int].plot(
            cmap=plt.cm.Blues_r, vmax=3.5, 
            cbar_kwargs={'label': 'Snow Height (m)'}
        )
    ax.scatter(260315, 4198989, color='black')
    ax.text(260615, 4199989, "SASP", color='black')

    # Second subplot -----------------------------
    ax2 = fig.add_subplot(1, 2, 2, projection='3d')
    ax2.view_init(60, view_angle)
    colors = get_colors(snow_ds['thickness'][day_int], plt.cm.Blues_r)

    dem['elevation (m)'].plot.surface(ax=ax2, facecolors=colors,
                           rcount=600, ccount=600, zorder=1)
    # locs for study plot location label
    x = np.array([260315, 260315])
    y = np.array([4198989, 4198989])
    z = np.array([3400, 4500])
    z2 = np.array([0, 3300])
 
    ax2.plot3D(x, y, z, zorder=4, color='black')
    ax2.plot3D(x, y, z2, zorder=2, color='black')
    ax2.text(260315, 4198989, 4600, "SASP", color='black', zorder=5)
    
    plt.savefig(f'{Path.home()}/skiles_storage/' \
                'AD_isnobal/out_plot/animation_wy2020_3d_test/' \
                f'{str((snow_ds["thickness"][day_int].time.dt.strftime("%Y%m%d").values))}.jpg',
                dpi=300)
    
    # testing .clf() for concurrency issue
    plt.close()
    fig.clf()
    
    
if __name__ == "__main__":
    main() 
