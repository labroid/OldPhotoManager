import photo_data

def test_instantiation():
    x = photo_data.photo_collection()
    assert isinstance(x, photo_data.photo_collection)
    
def test_scans_photos():
    photos = photo_data.photo_collection()