import photo_data
import os
import os.path

def test_instantiation():
    x = photo_data.photo_collection()
    assert isinstance(x, photo_data.photo_collection)
    
def test_scans_photos():
    photo_path = os.path.join(os.getcwd(), 'test_photos')
    photos = photo_data.get_photo_data(node_path=photo_path, pickle_path = None, node_update = True)