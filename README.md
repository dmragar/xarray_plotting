## Created animated plot of model outputs with xarray, dask, and matplotlib

Once frames have been created, animate with:
    
    ffmpeg -pattern_type glob -i '*.jpg' -crf 18 -filter:v "setpts=2.5*PTS" snow_depth.mp4
