"""Feature builders for the Golden Discovery Ranker v2 (prefetch-only hybrid).

Pure-python row->dict construction (no pandas in the scoring hot path; avoids the
per-row iterrows cost). MUST stay byte-identical to the training-time feature
construction used to fit the v2 artifact (see scripts that package the artifact):
generic URL features + per-domain regex rules + inlink (log) + anchor hand-crafted.
Post-fetch fields (title/json_ld/hreflang/redirect) are intentionally NOT built —
scoring happens at discovery time, before the page is fetched.
"""
from __future__ import annotations

import math
import re
from urllib.parse import urlsplit

# ---------------- per-domain regex (ported verbatim from training) ----------------
RE_YT_CHANNEL = re.compile(r'/channel/UC[\w-]{22}')
RE_YT_HANDLE = re.compile(r'/@[\w.-]+')
RE_YT_USER = re.compile(r'/user/[\w-]+')
RE_YT_WATCH_BASIC = re.compile(r'^v=[\w-]{11}(&|$)')
RE_NBA_GAME = re.compile(r'/game/[a-z]+-vs-[a-z]+-\d{10}')
RE_NBA_OLD_ARTICLE = re.compile(r'/article/\d{4}/\d{2}/')
RE_FS_MATCH_ROOT = re.compile(r'/match/[a-z]+/[\w-]+/[\w-]+/?$')
RE_FS_TEAM_ROOT = re.compile(r'/team/[\w-]+/[\w-]+/?$')
RE_GOOGLE_PLAY_APP = re.compile(r'play\.google\.com/store/apps/details')
RE_GOOGLE_SCHOLAR_CITE = re.compile(r'scholar\.google\.com/citations')
RE_SPOTIFY_ID = re.compile(r'/(show|artist|track|album|playlist|episode)/([\w]{22})')
RE_SPOTIFY_INTL = re.compile(r'/intl-[\w-]+/')
RE_WIKI_MAIN = re.compile(r'^/wiki/[^/:]+$')
RE_WIKI_SPECIAL = re.compile(r'/(Special|Spécial|Talk|Discussion|User|File|Category|Help|Template|Wikipedia):')
RE_YH_ARTICLE_HTML = re.compile(r'/articles/[\w-]+\.html$')
RE_YH_QUOTE_ROOT = re.compile(r'/quote/[\w.]+/?$')
RE_YHJP_NEWS_HASH = re.compile(r'news\.yahoo\.co\.jp/articles/[a-f0-9]{40}')
RE_YHJP_NEWS_PICKUP = re.compile(r'news\.yahoo\.co\.jp/pickup/\d+')
RE_YHJP_NEWS_EXPERT = re.compile(r'news\.yahoo\.co\.jp/expert/articles/[a-f0-9]+')
RE_YHJP_AUCTIONS = re.compile(r'auctions\.yahoo\.co\.jp/category/list/')
RE_YHJP_OLD_HEADLINES = re.compile(r'headlines\.yahoo\.co\.jp/hl\?a=')
RE_ESPN_ARTICLE = re.compile(r'/story/_/id/\d+/')
RE_ESPN_PLAYER = re.compile(r'/player/_/id/\d+/')
RE_ESPN_TEAM = re.compile(r'/team/_/name/[\w-]+/')
_RE_DATE_PATH = re.compile(r'/(19|20)\d{2}/\d{2}/')
RE_YEAR = re.compile(r'\b(19|20)\d{2}\b')
RE_DIGIT = re.compile(r'\d')
RE_GEN_DATE = re.compile(r'/(19|20)\d{2}[/-]\d{1,2}')


def features_url_generic(url: str) -> dict:
    parts = urlsplit(url)
    host = parts.netloc.lower()
    path = parts.path
    query = parts.query
    return {
        'url_length': len(url),
        'path_depth': path.count('/'),
        'query_count': url.count('?') + url.count('&'),
        'digit_ratio': sum(c.isdigit() for c in url) / max(len(url), 1),
        'is_https': 1 if parts.scheme == 'https' else 0,
        'subdomain': host.split('.', 1)[0] if host.count('.') >= 2 else 'root',
        'has_date_in_path': 1 if _RE_DATE_PATH.search(path) else 0,
        'host_lower': host, 'path_lower': path, 'query_lower': query,
    }


def rules_youtube(f):
    h, p, q = f['host_lower'], f['path_lower'], f['query_lower']
    return {'subdomain_is_www': 1 if h.startswith('www.') else 0,
            'is_channel_uc': 1 if RE_YT_CHANNEL.search(p) else 0,
            'is_handle_at': 1 if RE_YT_HANDLE.search(p) else 0,
            'is_user_path': 1 if RE_YT_USER.search(p) else 0,
            'is_custom_root': 1 if (p.count('/') == 1 and len(p) > 1 and 'watch' not in p and 'results' not in p) else 0,
            'is_watch_basic': 1 if (RE_YT_WATCH_BASIC.search(q) or re.search(r'v=[\w-]{11}', q)) else 0,
            'has_feature_param': 1 if 'feature=' in q else 0,
            'has_timestamp_param': 1 if re.search(r't=\d+s', q) else 0,
            'is_search_text': 1 if ('search_query=' in q or 'text=' in q) else 0}


def rules_yahoo_com(f):
    h, p, q = f['host_lower'], f['path_lower'], f['query_lower']
    return {'is_finance_subdomain': 1 if 'finance.' in h else 0,
            'is_sports_subdomain': 1 if 'sports.' in h else 0,
            'is_news_subdomain': 1 if 'news.' in h else 0,
            'is_add_my_subdomain': 1 if h.startswith('add.my.') else 0,
            'is_rd_subdomain': 1 if h.startswith('tw.rd.') or '.rd.' in h else 0,
            'is_article_html_path': 1 if RE_YH_ARTICLE_HTML.search(p) else 0,
            'is_quote_root_clean': 1 if (RE_YH_QUOTE_ROOT.search(p) and not q) else 0,
            'has_quote_subaction': 1 if ('/quote/' in p and len(p.split('/')) > 4) else 0}


def rules_yahoo_jp(f):
    url = f['host_lower'] + f['path_lower']
    return {'is_news_article_hash': 1 if RE_YHJP_NEWS_HASH.search(url) else 0,
            'is_news_pickup': 1 if RE_YHJP_NEWS_PICKUP.search(url) else 0,
            'is_news_expert': 1 if RE_YHJP_NEWS_EXPERT.search(url) else 0,
            'is_auctions_listing': 1 if RE_YHJP_AUCTIONS.search(url) else 0,
            'is_shopping_store': 1 if f['host_lower'].startswith('store.shopping.') else 0,
            'is_old_headlines': 1 if RE_YHJP_OLD_HEADLINES.search(url) else 0,
            'is_old_stocks_format': 1 if 'stocks.finance.yahoo.co.jp' in f['host_lower'] else 0,
            'is_baseball_player': 1 if 'baseball.yahoo.co.jp/npb/player/' in url else 0,
            'is_soccer_game': 1 if re.search(r'soccer\.yahoo\.co\.jp/(jleague|ws)/game/\d+', url) else 0}


def rules_espn(f):
    p = f['path_lower']
    return {'is_story_article': 1 if RE_ESPN_ARTICLE.search(p) else 0,
            'is_player_profile': 1 if RE_ESPN_PLAYER.search(p) else 0,
            'is_team_profile': 1 if RE_ESPN_TEAM.search(p) else 0,
            'is_scoreboard': 1 if '/scoreboard' in p else 0,
            'is_recap_game': 1 if '/recap/' in p or '/game/' in p else 0}


def rules_nba(f):
    h, p, q = f['host_lower'], f['path_lower'], f['query_lower']
    return {'subdomain_is_www': 1 if h.startswith('www.') else 0,
            'is_stats_subdomain_or_path': 1 if (h.startswith('stats.') or '/stats/' in p) else 0,
            'is_modern_game_id': 1 if RE_NBA_GAME.search(p) else 0,
            'is_team_news_path': 1 if re.search(r'/[a-z]+/news/', p) else 0,
            'is_old_article_format': 1 if RE_NBA_OLD_ARTICLE.search(p) else 0,
            'has_tracking_param': 1 if any(k in q for k in ['cid=', 'srsltid=', 'utm_']) else 0,
            'is_watch_video': 1 if '/watch/video/' in p or '/watch/' in p else 0}


def rules_spotify(f):
    h, p, q = f['host_lower'], f['path_lower'], f['query_lower']
    m = RE_SPOTIFY_ID.search(p)
    return {'is_open_subdomain': 1 if h.startswith('open.') else 0,
            'is_creators_subdomain': 1 if 'creators.spotify.com' in h else 0,
            'is_podcasters_subdomain': 1 if 'podcasters.spotify.com' in h else 0,
            'entity_type': m.group(1) if m else 'unknown',
            'spotify_id_well_formed': 1 if m else 0,
            'has_si_tracking': 1 if 'si=' in q else 0,
            'has_nd_param': 1 if 'nd=' in q else 0,
            'has_autoplay_param': 1 if 'autoplay=' in q else 0,
            'is_intl_subpath': 1 if RE_SPOTIFY_INTL.search(p) else 0}


def rules_wikipedia(f):
    h, p, q = f['host_lower'], f['path_lower'], f['query_lower']
    return {'lang_subdomain': h.split('.', 1)[0],
            'is_main_namespace': 1 if RE_WIKI_MAIN.search(p) else 0,
            'is_w_index_php': 1 if '/w/index.php' in p else 0,
            'has_action_param': 1 if 'action=' in q else 0,
            'is_special_or_talk': 1 if RE_WIKI_SPECIAL.search(p) else 0,
            'has_oldid_or_section': 1 if 'oldid=' in q or 'section=' in q else 0}


RULES_MAP = {
    'youtube.com': rules_youtube, 'yahoo.com': rules_yahoo_com, 'yahoo.co.jp': rules_yahoo_jp,
    'espn.com': rules_espn, 'nba.com': rules_nba, 'spotify.com': rules_spotify, 'wikipedia.org': rules_wikipedia,
}


def _anchor_str(a) -> str:
    if a is None:
        return ''
    s = str(a)
    return '' if s == 'nan' else s


def anchor_features(a: str) -> dict:
    return {'anchor_has': 1 if a else 0, 'anchor_length': len(a),
            'anchor_word_count': len(a.split()) if a else 0,
            'anchor_has_digit': 1 if RE_DIGIT.search(a) else 0,
            'anchor_has_year': 1 if RE_YEAR.search(a) else 0}


def specialized_feature_row(row: dict, rules_fn) -> dict:
    """Prefetch-only feature dict for a specialized-domain URL.
    Matches training: generic + rules + log-inlink + anchor (post-fetch dropped)."""
    f = features_url_generic(row['url'])
    f.update(rules_fn(f))
    f.pop('host_lower'); f.pop('path_lower'); f.pop('query_lower')
    ia = row.get('inlink_count_approx'); ie = row.get('inlink_count_external')
    f['log_inlink_approx'] = math.log1p(float(ia or 0))
    f['log_inlink_external'] = math.log1p(float(ie or 0))
    f['has_external_inlink'] = 1 if (ie or 0) > 0 else 0
    f.update(anchor_features(_anchor_str(row.get('anchor_text'))))
    return f


# ---- general head features (ported from training gfeats) ----
GENERAL_FEATCOLS = ['url_length', 'digit_ratio', 'path_depth', 'query_count', 'is_https',
                    'has_date', 'n_dots', 'log_inlink', 'log_inlink_ext', 'has_ext_inlink',
                    'anchor_has', 'anchor_len', 'anchor_wc', 'anchor_digit', 'anchor_year']


def general_feature_row(row: dict) -> dict:
    u = row['url']; p = urlsplit(u); host = p.netloc.lower(); path = p.path; qy = p.query
    a = _anchor_str(row.get('anchor_text'))
    ia = row.get('inlink_count_approx'); ie = row.get('inlink_count_external')
    return {'url_length': len(u), 'digit_ratio': sum(c.isdigit() for c in u) / max(len(u), 1),
            'path_depth': path.count('/'), 'query_count': qy.count('=') if qy else 0,
            'is_https': 1 if u.startswith('https') else 0, 'has_date': 1 if RE_GEN_DATE.search(path) else 0,
            'n_dots': host.count('.'), 'log_inlink': math.log1p(float(ia or 0)),
            'log_inlink_ext': math.log1p(float(ie or 0)), 'has_ext_inlink': 1 if (ie or 0) > 0 else 0,
            'anchor_has': 1 if a else 0, 'anchor_len': len(a), 'anchor_wc': len(a.split()) if a else 0,
            'anchor_digit': 1 if RE_DIGIT.search(a) else 0, 'anchor_year': 1 if RE_YEAR.search(a) else 0}


def registrable_domain(u: str) -> str:
    h = urlsplit(u).netloc.lower()
    if h.startswith('www.'):
        h = h[4:]
    parts = h.split('.')
    if len(parts) >= 3 and parts[-2] in ('co', 'com', 'org', 'net', 'gov', 'ac'):
        return '.'.join(parts[-3:])
    return '.'.join(parts[-2:]) if len(parts) >= 2 else h
