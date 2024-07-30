import sys

class Utils:
    @staticmethod
    async def prep_url(id_val: str, id_type: str):        
        if id_type == 'user':
            id_val = (f"https://www.youtube.com/@{id_val}/videos")
        elif id_type == 'user_playlist':
            id_val = (f"https://www.youtube.com/@{id_val}/playlists")
        elif id_type == 'playlist':
            id_val = (f"https://www.youtube.com/playlist?list={id_val}")        
        pass
        return id_val