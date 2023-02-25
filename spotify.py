import logging
import re
from time import sleep

import tekore as tk

logger = logging.getLogger(__name__)

def search_normalize(string):
    return re.sub(r'[^\w\s\.\/]', '', re.sub(r'[-]', ' ', string.lower()))


def compare_normalize(string):
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', string.lower()))


class SpotifyClient:
    callback_url = 'https://example.com/callback'
    user_scope = (
        tk.scope.playlist_modify_private +
        tk.scope.playlist_modify_public
    )

    def __init__(self, client_id, client_secret):
        # TODO implement web user auth flow
        refresh_token = tk.config_from_file('spotify.conf', return_refresh=True)[-1]
        self.token = tk.refresh_user_token(client_id, client_secret, refresh_token)
        self.client = tk.Spotify(self.token)
        self.user = self.client.current_user()

    def query(self, query):
        query_str = ' '.join(f'{k}:"{search_normalize(v)}"' for k, v in query.items())
        results, = self.client.search(query_str, types=('track',))
        for result in results.items:
            found_track = found_artist = found_album = None
            if compare_normalize(query['track']) in compare_normalize(result.name):
                found_track = result

                for artist in result.artists:
                    if compare_normalize(artist.name) == compare_normalize(query['artist']):
                        found_artist = artist
                if found_track and found_artist:
                    if 'album' in query:
                        if compare_normalize(query['album']) == compare_normalize(result.album.name):
                            return found_track, found_artist, result.album
                    else:
                        return found_track, found_artist, None

    def query_albums(self, track):
        query_str = f'''artist:"{search_normalize(track['artist'])}" album:"{search_normalize(track['album'])}"'''
        results, = self.client.search(query_str, types=('album',))
        for result in results.items:
            tracks = [t for t in self.client.album_tracks(result.id).items
                    if compare_normalize(track['name']) == compare_normalize(t.name)]
            if tracks:
                match_track = tracks[0]
                for artist in match_track.artists:
                    if compare_normalize(artist.name) == compare_normalize(track['artist']):
                        return match_track, artist, result

    def create_playlist(self, name, track_ids, public=True, description=''):
        playlist = self.client.playlist_create(self.user.id, name, public, description)
        self.add_tracks_to_playlist(playlist.id, track_ids)
        return playlist

    def add_tracks_to_playlist(self, playlist_id, track_ids=[]):
        batch_size = 100
        cur = 0
        stop = len(track_ids)
        while cur < stop:
            uris = [f'spotify:track:{i}' for i in track_ids[cur:cur+batch_size]]
            self.client.playlist_add(playlist_id, uris)
            cur += batch_size

    def find_track_details(self, track, trys=0):
        try:
            match = self.query(
                {'artist': track['artist'], 'track': track['name'], 'album': track['album']})
            if not match and track['album']:
                match = self.query_albums(track)
            if not match:
                match = self.query(
                    {'artist': track['artist'], 'track': track['name']})
        except tk.TooManyRequests as err:
            if trys <= 3:
                wait = int(err.response.headers.get('retry-after', 60))
                sleep(wait * 2)
                logger.warning(f'rate limit hit waiting: {wait}')
                return self.find_track_details(track, trys=trys + 1)
        except Exception:
            if trys < 2:
                logger.warning('error retrying in 5 secs')
                sleep(5)
                return self.find_track_details(track, trys=trys + 1)
            raise

        if match:
            match_track, match_artist, match_album = match
            track['track_spotify_id'] = match_track.id
            track['artist_spotify_id'] = match_artist.id
            if match_album:
                track['album_spotify_id'] = match_album.id
            if not track['album']:
                track['album'] = match_track.album.name
                track['date'] = match_track.album.release_date
        return track
