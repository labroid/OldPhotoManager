import cProfile
import photo_data

cProfile.run('photo_data.main()', sort = 'cumulative'  )