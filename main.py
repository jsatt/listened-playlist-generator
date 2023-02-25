import json
import logging
import re
from csv import DictReader, DictWriter
from time import sleep

import arrow
import musicbrainzngs as mb
import pandas as pd
from dotenv import dotenv_values

from lastfm import LastFmClient
from spotify import SpotifyClient


VERSION = (0, 0, 1)
__version__ = '.'.join(map(str, VERSION))

# logging.basicConfig(level='DEBUG')

logger = logging.getLogger(__name__)
env = dotenv_values('.env')

LASTFM_API_KEY = env['LASTFM_API_KEY']
LASTFM_API_URL = env.get('LASTFM_API_URL', 'https://ws.audioscrobbler.com/2.0/')
LASTFM_USERNAME = env['LASTFM_USERNAME']
# MUSICBRAINZ_CLIENT_ID = env['MUSICBRAINZ_CLIENT_ID']
# MUSICBRAINZ_CLIENT_SECRET = env['MUSICBRAINZ_CLIENT_SECRET']
MUSICBRAINZ_USERAGENT = env.get('MUSICBRAINZ_USERAGENT', 'lastfm-scrobbler')
MUSICBRAINZ_APP_VERSION = env.get('MUSICBRAINZ_APP_VERSION', __version__)
TRACKS_FILE = 'data/tracks.csv'
ALBUMS_FILE = 'data/albums.csv'
ALBUM_YEAR_FILE_TEMPLATE = 'data/albums_{year}.csv'
YEAR_PLAYLIST_FILE_TEMPLATE = 'data/playlist_{year}.csv'
DECADE_PLAYLIST_FILE_TEMPLATE = 'data/playlist_{year}s.csv'
DETAILED_FILE = 'data/details.csv'
FINAL_FILE = 'data/final.csv'
MISSING_SPOTIFY_FILE_TEMPLATE = 'data/playlist_missing_{year}.csv'

SPOTIFY_CLIENT_ID = env.get('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = env.get('SPOTIFY_CLIENT_SECRET')
SPOTIFY_USER_ID = env.get('SPOTIFY_USER_ID')

mb.set_useragent(MUSICBRAINZ_USERAGENT, MUSICBRAINZ_APP_VERSION)

COUNTRY_MAP = {
    'US': 100,
    'GB': 99,
    'AU': 98,
    'AT': 80,
    'BE': 80,
    'JP': 70,
}

ALBUM_TYPE_MAP = {
    'Album': 100,
    'EP': 99,
    'Single': 98,
    # 'Compilation': 97,
}


def write_csv(path, records, fieldnames):
    with open(path, 'w', newline='') as f:
        csv = DictWriter(f, fieldnames=fieldnames, dialect='unix')
        csv.writeheader()
        # for record in records:
        csv.writerows(records)


def read_csv(path):
    with open(path, 'r') as f:
        csv = DictReader(f)
        for row in csv:
            yield row


def collect_tracks(period=None, limit=None, **params):
    client = LastFmClient(LASTFM_API_KEY, LASTFM_USERNAME)
    fieldnames = ['name', 'artist', 'artist_mbid', 'playcount', 'rank']
    tracks = (
        {'name': t['name'], 'artist': t['artist']['name'], 'artist_mbid': t['artist']['mbid'],
         'playcount': t['playcount'], 'rank': t['@attr']['rank']}
        for idx, t in enumerate(client.get_user_top_tracks(period=period, limit=limit, **params))
    )
    write_csv(TRACKS_FILE, tracks, fieldnames)


def sorta_match(orig, test):
    replace_map = {
        '\u2019': "'",
        '\u201b': "'",
        '\u201c': '"',
        '\u201d': '"',
    }
    trans = str.maketrans(replace_map)
    return orig.translate(trans).lower() == test.translate(trans).lower()


def build_mb_query(track):
    query = [
        f"arid:\"{track['artist_mbid']}\"",
        f"recordingaccent:\"{track['name']}\"",
        # 'primarytype:Album',
    ]
    return ' AND '.join(query)


def query_mb(track, limit=100, page_limit=10):
    query = build_mb_query(track)
    current_page = 0
    while True:
        page = mb.search_recordings(query, limit=limit, offset=(current_page * limit))
        recordings = page.get('recording-list', [])
        for recording in recordings:
            yield recording
        if len(recordings) < limit or current_page >= page_limit:
            break
        current_page += 1


def validate_track(track):
    return all([
        (track['artist_mbid'] not in ('', '-', ' ')),
    ])


def get_date(date_string):
    try:
        return arrow.get(date_string, 'YYYY-MM-DD')
    except arrow.ParserError:
        try:
            # End of month
            return arrow.get(date_string, 'YYYY-MM').shift(months=+1, microseconds=-1)
        except arrow.ParserError:
            try:
                return arrow.get(date_string, 'YYYY').shift(years=+1, microseconds=-1)
            except arrow.ParserError:
                return arrow.get('9999-12-31')


def rank_releases(track, recordings, exclude={}):
    for recording in recordings:
        for release in recording.get('release-list', []):
            group = release.get('release-group')
            album_type = group.get('secondary-type-list', [group.get('primary-type')])[0]
            artist = recording.get('artist-credit', [{}])[0].get('artist', {}).get('sort-name', '')
            date = get_date(release.get('date', ''))
            record = {
                'title': recording['title'],
                'date': date.format('YYYY-MM-DD'),
                'country': release.get('country', ''),
                'artist': artist,
                'album': release.get('title', ''),
                'album_type': album_type,
                'album_mbid': release.get('id', ''),
                'track_mbid': recording.get('id', ''),
                'sort_key': (
                    -int(sorta_match(track['name'], recording['title'])),
                    -ALBUM_TYPE_MAP.get(album_type, 0),
                    -COUNTRY_MAP.get(release.get('country'), 0),
                    date.format('YYYY-MM-DD HH:mm:ss.SSSSSSS'),
                ),
            }
            if any([record[k] == v for k, v in exclude.items()]):
                break
            if album_type in ALBUM_TYPE_MAP: #'Album':
                yield record


def find_release(track, exclude={}):
    if validate_track(track):
        recordings = query_mb(track)
        releases = list(rank_releases(track, recordings, exclude=exclude))
        if releases:
            logger.debug(f"Releases Found ({track['name']} - {track.get('artist')}): {json.dumps(releases)}")
            releases.sort(key=lambda x: x['sort_key'])
            release = releases[0]
            track['album'] = release.get('album', '')
            track['date'] = release.get('date', '')
            track['album_mbid'] = release.get('album_mbid', '')
            track['track_mbid'] = release.get('track_mbid', '')
    return track


def get_release_data(max_records=None):
    tracks = (
        find_release(t)
        for idx, t in enumerate(read_csv(TRACKS_FILE))
        if not max_records or idx < max_records
    )
    fieldnames = ['name', 'artist', 'artist_mbid', 'playcount', 'rank', 'album',
                  'album_mbid', 'track_mbid', 'date']
    write_csv(DETAILED_FILE, tracks, fieldnames)


def calculate_top_albums(for_year=None, write=True):
    df = pd.read_csv(DETAILED_FILE).drop(columns=['rank'])
    df = df.replace({'date': '9999-12-31'}, value=pd.NA)
    df['year'] = pd.to_datetime(df['date']).dt.to_period('Y').astype(pd.Int64Dtype()) + 1970
    df = (df.groupby(['artist', 'album'])  # get artist and album
          .agg({'year': 'min',  # assume earliest year, ie count deluxe with orig date
                'name': 'nunique',  # total tracks on album
                'playcount': 'sum'})  # total playcount for all tracks on album
          .sort_values('playcount', ascending=False))  # give us most listens first

    if for_year:
          df = df.query(f'year == {for_year}')
          filename = ALBUM_YEAR_FILE_TEMPLATE.format(year=for_year)
    else:
          filename = ALBUMS_FILE

    if write:
        df.to_csv(filename)


def get_top_tracks(query):
    df = pd.read_csv(FINAL_FILE).drop(columns=['rank'])
    df = df.replace({'date': '9999-12-31'}, value=pd.NA)
    df['year'] = pd.to_datetime(df['date']).dt.to_period('Y').astype(pd.Int64Dtype()) + 1970
    df = (df.groupby(['name', 'artist', 'album', 'year', 'track_spotify_id'])  # get artist and album
          .agg({'playcount': 'sum'})  # total playcount for all tracks on album
          .sort_values('playcount', ascending=False))  # give us most listens first
    df = df.query(query)
    return df


def gen_year_playlist(for_year, play_threshold=5, write=True):
    df = get_top_tracks(f'year == {for_year} & playcount >= {play_threshold}')
    filename = YEAR_PLAYLIST_FILE_TEMPLATE.format(year=for_year)

    if write:
        df.to_csv(filename)
    else:
        df = df.reset_index()
        for _, r in df.iterrows():
            print(list(r.to_dict().values()))


def gen_decade_playlist(start_year, play_threshold=8, write=True):
    df = get_top_tracks(
        f'year >= {start_year} & year < {start_year + 10} & playcount >= {play_threshold}')
    filename = DECADE_PLAYLIST_FILE_TEMPLATE.format(year=start_year)

    if write:
        df.to_csv(filename)
    else:
        df = df.reset_index()
        for _, r in df.iterrows():
            print(list(r.to_dict().values()))


def get_spotify_data(max_records=None):
    client = SpotifyClient(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
    tracks = (
        client.find_track_details(t)
        for idx, t in enumerate(read_csv(DETAILED_FILE))
        if not max_records or idx < max_records
    )
    fieldnames = ['name', 'artist', 'artist_mbid', 'playcount', 'rank', 'album',
                  'album_mbid', 'track_mbid', 'date', 'track_spotify_id',
                  'album_spotify_id', 'artist_spotify_id']
    write_csv(FINAL_FILE, tracks, fieldnames)


def gen_spotify_yearly_playlist(year):
    df = pd.read_csv(YEAR_PLAYLIST_FILE_TEMPLATE.format(year=year))
    client = SpotifyClient(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
    # client.authorize_user(SPOTIFY_USER_ID)
    track_ids = df[df['track_spotify_id'].notna()]['track_spotify_id'].to_list()
    name = f'{year} Favorite Tracks'
    client.create_playlist(name, track_ids)

    na_tracks = df[df['track_spotify_id'].isna()]
    if na_tracks.any()[0]:
        df.to_csv(MISSING_SPOTIFY_FILE_TEMPLATE.format(year=year))
