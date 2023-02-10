import pandas as pd
from tiktokapipy.api import TikTokAPI

import warnings
warnings.simplefilter(action='ignore')

challengenames = "shampoingsolide"
challengenames = challengenames.split(',')
try:
    df = pd.read_csv('./res_tiktok.csv', sep=";")
except:
    df = pd.DataFrame(columns=['Tag Name', 'Tag Id', 'Tag Description', 'Tag Is Commercial',
     'Tag View Count', 'Post Id', 'Post Url', 'Post Author', 'Post Created Time',
     'Post Creator Id', 'Post Creator Nickname', 'Post Creator Name', 'Post Creator Verified',
     'Post Creator Private', 'Post Creator Sec UID', 'Post Creator Likes Given',
     'Post Creator Likes Received','Post Creator Follower Count', 'Post Creator Following Count',
     'Post Creator Video Count','Post Description', 'Post Like Count','Post Play Count',
     'Post Comment Count', 'Post Share Count','Post Comments','Post Hastag',
     'Post Labels','Post Image','Post Music'])
print("Tags: ", challengenames)
with TikTokAPI(scroll_down_time=150,
               emulate_mobile=False,
               navigation_retries=2,
               navigation_timeout=10,
               headless=True) as api:
    for challengename in challengenames:
        api.default_scroll_down_time = 150
        print("Processing Tag: ", challengename)
        challenges = api.challenge(challengename)
        challenge_id = 'ID' + str(challenges.id)
        challenge_view_count = challenges.stats.view_count
        challenge_description = challenges.desc
        challenge_iscommerce = challenges.is_commerce

        videos_id = [str(x.id) for x in challenges.videos.sorted_by(key=lambda vid: vid.stats.digg_count, reverse=True).light_models]

        challenge_videos = challenges.videos.sorted_by(key=lambda vid: vid.stats.digg_count, reverse=True)

        api.default_scroll_down_time = 0

        print(f'{len(videos_id)} retrieve.')

        for ind, video_id in enumerate(videos_id):
            try:
                if len(df[(df['Tag Name']==challengename) & (df['Post Id']==("ID" + video_id))]) > 0:
                    continue
                challenge = challenge_videos.fetch(ind)
                challenge_video_id = "ID" + video_id
                challenge_video_image = challenge.video.cover
                challenge_video_like_count = challenge.stats.digg_count
                challenge_video_play_count = challenge.stats.play_count
                challenge_video_comment_count = challenge.stats.comment_count
                challenge_video_share_count = challenge.stats.share_count
                challenge_video_comments = challenge.comments
                challenge_video_create_time = challenge.create_time
                challenge_video_description = challenge.desc
                challenge_video_labels = challenge.diversification_labels
                challenge_video_music = challenge.music
                challenge_video_tag = [x.title for x in challenge.challenges]
                try:
                    challenge_video_creator = challenge.creator()
                except:
                    challenge_video_creator = None
                try:
                    challenge_video_author = "ID" + str(challenge.author.unique_id)
                except:
                    challenge_video_author = ''
                if challenge_video_creator is None:
                    challenge_video_creator_id = '' 
                    challenge_video_creator_nickname = ''
                    challenge_video_creator_private = ''
                    challenge_video_creator_sec_uid = ''
                    challenge_video_creator_like_gives = 0
                    challenge_video_creator_like_count = 0
                    challenge_video_creator_follower_count = 0
                    challenge_video_creator_following_count = 0
                    challenge_video_creator_video_count = 0
                    challenge_video_creator_verified = False
                    challenge_video_creator_unique_id = ''
                else:
                    challenge_video_creator_id = "ID" + str(challenge_video_creator.id)
                    challenge_video_creator_nickname = challenge_video_creator.nickname
                    challenge_video_creator_private = challenge_video_creator.private_account
                    try:
                        challenge_video_creator_sec_uid = str(challenge_video_creator.sec_uid)
                    except:
                        challenge_video_creator_sec_uid = ''
                    challenge_video_creator_like_gives = challenge_video_creator.stats.digg_count 
                    challenge_video_creator_like_count = challenge_video_creator.stats.heart_count
                    challenge_video_creator_follower_count = challenge_video_creator.stats.follower_count
                    challenge_video_creator_following_count = challenge_video_creator.stats.following_count
                    challenge_video_creator_video_count = challenge_video_creator.stats.video_count
                    challenge_video_creator_verified = challenge_video_creator.verified
                    challenge_video_creator_unique_id = challenge_video_creator.unique_id

                challenge_video_url = f"https://www.tiktok.com/@{challenge_video_creator_unique_id}/video/{challenge_video_id[2:]}"
    
                print(f"{len(df)+1} -> {challenge_video_url}")

                df.loc[len(df),:] = [challengename, challenge_id, challenge_description, challenge_iscommerce,
                                        challenge_view_count, challenge_video_id, challenge_video_url,
                                        challenge_video_author, challenge_video_create_time,
                                        challenge_video_creator_id, challenge_video_creator_unique_id,
                                        challenge_video_creator_verified, challenge_video_creator_nickname,
                                        challenge_video_creator_private, challenge_video_creator_sec_uid,
                                        challenge_video_creator_like_gives, challenge_video_creator_like_count,
                                        challenge_video_creator_follower_count, challenge_video_creator_following_count,
                                        challenge_video_creator_video_count, challenge_video_description,
                                        challenge_video_like_count, challenge_video_play_count,
                                        challenge_video_comment_count, challenge_video_share_count,
                                        challenge_video_comments,challenge_video_tag, challenge_video_labels,
                                        challenge_video_image, challenge_video_music]
                if len(df) % 25 == 0:
                    df.to_csv('./res_tiktok.csv', sep=";", index=False, encoding='utf-8-sig')
            except:
                pass

df.to_csv('./res_tiktok.csv', sep=";", index=False, encoding='utf-8-sig')