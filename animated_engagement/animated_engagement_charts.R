library(bigrquery)
library(DBI)
library(tidyverse)
library(gganimate)
library(scales)
library(magick)
library(ggdark)
library(cowplot)


# import lens logo to add to plots
lens_png = image_read("lens_logo.png")


run_for_user = function(current_handle = 'rickydata.lens'){
  # connect to bigquery
  bquery_con <- dbConnect(
    bigrquery::bigquery(),
    project = "lens-public-data",
    dataset = "polygon",
    billing = "mlflow-291816"
  )
  
  # query for account id first
  account_id = paste0("select
                      profile_id,
                      owned_by,
                      handle,
                      profile_picture_s3_url
                      from `lens-public-data.polygon.public_profile`
                      where handle = '",current_handle,"'
                    limit 1")
  
  # Download data
  account_id = dbGetQuery(bquery_con, account_id)$profile_id
  
  # query
  sql_query = paste0("WITH notification_data AS (
  SELECT
    TIMESTAMP_TRUNC(notification_action_date, DAY) AS date,
    notification_receiving_profile_id,
    COALESCE(notification_sender_wallet_address, notification_sender_profile_id) AS sender_id,
    COUNT(*) AS count
  FROM `lens-public-data.polygon.public_notification`
  WHERE notification_receiving_profile_id = '",account_id,"'
  GROUP BY date, notification_receiving_profile_id, sender_id
),
calendar AS (
  SELECT DISTINCT
    TIMESTAMP_TRUNC(notification_action_date, DAY) AS date
  FROM `lens-public-data.polygon.public_notification`
  WHERE notification_receiving_profile_id = '",account_id,"'
),
senders AS (
  SELECT DISTINCT
    COALESCE(notification_sender_wallet_address, notification_sender_profile_id) AS sender_id
  FROM `lens-public-data.polygon.public_notification`
  WHERE notification_receiving_profile_id = '",account_id,"'
),
cumulative_interactions AS (
  SELECT
    calendar.date,
    '",account_id,"' AS notification_receiving_profile_id,
    senders.sender_id,
    COALESCE(
      SUM(
        COALESCE(notification_data.count, 0)
      ) OVER (
        PARTITION BY senders.sender_id
        ORDER BY calendar.date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),
      0
    ) AS cumulative_interaction_count
  FROM calendar
  CROSS JOIN senders
  LEFT JOIN notification_data
    ON calendar.date = notification_data.date
    AND senders.sender_id = notification_data.sender_id
)
SELECT
  cumulative_interactions.date,
  cumulative_interactions.notification_receiving_profile_id,
  cumulative_interactions.sender_id,
  cumulative_interactions.cumulative_interaction_count,
  public_profile.profile_id,
  public_profile.owned_by,
  public_profile.handle,
  public_profile.profile_picture_s3_url
FROM cumulative_interactions
JOIN `lens-public-data.polygon.public_profile` AS public_profile
  ON cumulative_interactions.sender_id = public_profile.profile_id
  OR cumulative_interactions.sender_id = public_profile.owned_by
ORDER BY cumulative_interactions.date DESC")


# Download data
data = dbGetQuery(bquery_con, sql_query)

# disconnect
dbDisconnect(bquery_con)



# GREAT EXAMPLE: https://towardsdatascience.com/create-animated-bar-charts-using-r-31d09e5841da

# remove engagement by user themselves (for example responding to comments)
data = filter(data, notification_receiving_profile_id != sender_id, owned_by != sender_id)


# calculate rankings
data_formatted <- data %>%
  filter(cumulative_interaction_count > 5) %>%
  arrange(date, desc(cumulative_interaction_count), handle) %>% # Order by date, count and also handle
  group_by(date) %>%
  mutate(rank = dense_rank(desc(cumulative_interaction_count) + row_number() / 1000),
         Value_rel = cumulative_interaction_count / cumulative_interaction_count[rank == 1],
         Value_lbl = cumulative_interaction_count) %>%
  group_by(handle) %>%
  filter(rank <= 10) %>%
  ungroup()

staticplot <- ggplot(data_formatted, aes(rank, group = handle, 
                                         fill = as.factor(handle), color = as.factor(handle))) +
  geom_tile(aes(y = cumulative_interaction_count/2,
                height = cumulative_interaction_count,
                width = 0.9), alpha = 0.8, color = NA) +
  geom_text(aes(y = 0, label = paste(handle, " ")), vjust = 0.2, hjust = 1) +
  geom_text(aes(y = round(cumulative_interaction_count), 
                label = round(Value_lbl), 
                hjust = 0),
            size = 3.5, fontface = "bold") +
  coord_flip(clip = "off", expand = FALSE) +
  scale_x_reverse() +
  guides(color = FALSE, fill = FALSE) +
  theme(axis.line=element_blank(),
        axis.text.x=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks=element_blank(),
        axis.title.x=element_blank(),
        axis.title.y=element_blank(),
        legend.position="none",
        panel.background=element_blank(),
        panel.border=element_blank(),
        panel.grid.major=element_blank(),
        panel.grid.minor=element_blank(),
        panel.grid.major.x = element_line(size=.1, color="grey"),
        panel.grid.minor.x = element_line(size=.1, color="grey"),
        plot.title=element_text(size=20, hjust=0.5, face="bold"),
        plot.subtitle=element_text(size=18, hjust=0.5, face="italic"),
        plot.caption =element_text(size=8, hjust=0.5, face="italic"),
        plot.background=element_blank(),
        plot.margin = margin(2, 2, 2, 4, "cm")) +
  labs(title = paste0("Top Profiles Engaging With ",current_handle),
       subtitle = "Through: {closest_state}",
       caption = "Made by rickydata.lens") +
  theme(plot.caption = element_text(size = 16, hjust = 1, vjust = 1, face = "bold")) 
# dark_theme_minimal() 

# Add logo (makes it super slow!)
# staticplot <- ggdraw() +
#   draw_plot(staticplot) +
#   draw_image(lens_png, x = 0.42, y = 0.32, scale = 0.2)


anim = staticplot + transition_states(date, transition_length = 9000, state_length = 50) +
  view_follow(fixed_x = TRUE) +
  labs(title = paste0("Top Profiles Engaging With ",current_handle),
       caption = "Made by rickydata.lens")


# show but don't save:
# animate(anim, length(unique(data$date)), fps = 20, width = 800, height = 670,
#         duration = 90, end_pause = 60, res = 100)


# For GIF
animate(anim, length(unique(data$date)), fps = 20, width = 800, height = 670,
        duration = 90, end_pause = 0, res = 100, # can lower resolution to upload image if needed
        renderer = gifski_renderer(paste0(current_handle,".gif")))

# CONVERT MANUALLY HERE IF LARGER THAN 10MB: https://convertio.co/gif-mp4/


# For MP4 (doesn't work)
# library(av)
# b <- animate(anim, length(unique(data$date)), fps = 20, width = 800, height = 670,
#              duration = 90, end_pause = 60, res = 100, renderer = av_renderer())
# anim_save("rickydata.mp4", b)






# Next: make a similar one showing most content I have interacted with myself




# Next: make a similar one showing most content I have interacted with myself


}

# Run script for a user:
# run_for_user('punkess.lens')
# run_for_user('stani.lens') # NEED TO COME BACK TO STANI - TOO MUCH DATA
# run_for_user('mazemari.lens')
# run_for_user('carlosbeltran.lens')
# run_for_user('larryscruff.lens')
# run_for_user('kipto.lens')
# run_for_user('jovana_kvrzic.lens')
# run_for_user('nader.lens')
# run_for_user('gotenks.lens')
# run_for_user('0xzelda.lens')
run_for_user('bennyj504.lens')

# After: convert to MP4 here: https://convertio.co/gif-mp4/





# Alternative query which gets output much lower so I don't need to download millions of rows of data (could be useful if I made it into an app): 

#   sql_query = paste0("
# WITH notification_data AS (
#   SELECT
#   TIMESTAMP_TRUNC(notification_action_date, DAY) AS date,
#   notification_receiving_profile_id,
#   COALESCE(notification_sender_wallet_address, notification_sender_profile_id) AS sender_id,
#   COUNT(*) AS count
#   FROM `lens-public-data.polygon.public_notification`
#   WHERE notification_receiving_profile_id = '", account_id, "'
#   GROUP BY date, notification_receiving_profile_id, sender_id
# ),
# calendar AS (
#   SELECT DISTINCT
#   TIMESTAMP_TRUNC(notification_action_date, DAY) AS date
#   FROM `lens-public-data.polygon.public_notification`
#   WHERE notification_receiving_profile_id = '", account_id, "'
# ),
# senders AS (
#   SELECT DISTINCT
#   COALESCE(notification_sender_wallet_address, notification_sender_profile_id) AS sender_id
#   FROM `lens-public-data.polygon.public_notification`
#   WHERE notification_receiving_profile_id = '", account_id, "'
# ),
# cumulative_interactions AS (
#   SELECT
#   calendar.date,
#   '", account_id, "' AS notification_receiving_profile_id,
#   senders.sender_id,
#   COALESCE(
#     SUM(
#       COALESCE(notification_data.count, 0)
#     ) OVER (
#       PARTITION BY senders.sender_id
#       ORDER BY calendar.date ASC
#       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
#     ),
#     0
#   ) AS cumulative_interaction_count
#   FROM calendar
#   CROSS JOIN senders
#   LEFT JOIN notification_data
#   ON calendar.date = notification_data.date
#   AND senders.sender_id = notification_data.sender_id
# ),
# filtered_data AS (
#   SELECT
#   cumulative_interactions.date,
#   cumulative_interactions.notification_receiving_profile_id,
#   cumulative_interactions.sender_id,
#   cumulative_interactions.cumulative_interaction_count,
#   public_profile.profile_id,
#   public_profile.owned_by,
#   public_profile.handle,
#   public_profile.profile_picture_s3_url
#   FROM cumulative_interactions
#   JOIN `lens-public-data.polygon.public_profile` AS public_profile
#   ON cumulative_interactions.sender_id = public_profile.profile_id
#   OR cumulative_interactions.sender_id = public_profile.owned_by
# ),
# processed_data AS (
#   SELECT
#   filtered_data.*,
#   ROW_NUMBER() OVER (PARTITION BY filtered_data.date ORDER BY filtered_data.cumulative_interaction_count DESC, filtered_data.handle ASC) AS row_num
#   FROM filtered_data
#   WHERE filtered_data.notification_receiving_profile_id <> filtered_data.sender_id
#   AND filtered_data.owned_by <> filtered_data.sender_id
# ),
# ranked_data AS (
#   SELECT
#   processed_data.*,
#   DENSE_RANK() OVER (PARTITION BY processed_data.date ORDER BY processed_data.cumulative_interaction_count DESC, processed_data.handle ASC) AS rank
#   FROM processed_data
# )
# SELECT
# ranked_data.date,
# ranked_data.notification_receiving_profile_id,
# ranked_data.sender_id,
# ranked_data.cumulative_interaction_count,
# ranked_data.profile_id,
# ranked_data.owned_by,
# ranked_data.handle,
# ranked_data.profile_picture_s3_url,
# ranked_data.rank,
# ranked_data.cumulative_interaction_count / MAX(CASE WHEN ranked_data.rank = 1 THEN ranked_data.cumulative_interaction_count END) OVER (PARTITION BY ranked_data.date) AS Value_rel,
# ranked_data.cumulative_interaction_count AS Value_lbl
# FROM ranked_data
# WHERE ranked_data.row_num <= 10
# ORDER BY ranked_data.date DESC")









# Next query - show your own engagement with other users ------------------


run_for_user_interactions = function(current_handle = 'rickydata.lens'){
  # connect to bigquery
  bquery_con <- dbConnect(
    bigrquery::bigquery(),
    project = "lens-public-data",
    dataset = "polygon",
    billing = "mlflow-291816"
  )
  
  # query for account id first
  account_id = paste0("select
                      profile_id,
                      owned_by,
                      handle,
                      profile_picture_s3_url
                      from `lens-public-data.polygon.public_profile`
                      where handle = '",current_handle,"'
                    limit 1")
  
  # Download data
  account_id = dbGetQuery(bquery_con, account_id)$profile_id
  
  # query
  sql_query = paste0("WITH notification_data AS (
  SELECT
    TIMESTAMP_TRUNC(notification_action_date, DAY) AS date,
    notification_sender_profile_id,
    notification_receiving_profile_id AS receiver_id,
    COUNT(*) AS count
  FROM `lens-public-data.polygon.public_notification`
  WHERE notification_sender_profile_id = '",account_id,"'
  GROUP BY date, notification_sender_profile_id, receiver_id
),
calendar AS (
  SELECT DISTINCT
    TIMESTAMP_TRUNC(notification_action_date, DAY) AS date
  FROM `lens-public-data.polygon.public_notification`
  WHERE notification_sender_profile_id = '",account_id,"'
),
receivers AS (
  SELECT DISTINCT
    notification_receiving_profile_id AS receiver_id
  FROM `lens-public-data.polygon.public_notification`
  WHERE notification_sender_profile_id = '",account_id,"'
),
cumulative_interactions AS (
  SELECT
    calendar.date,
    '",account_id,"' AS notification_sender_profile_id,
    receivers.receiver_id,
    COALESCE(
      SUM(
        COALESCE(notification_data.count, 0)
      ) OVER (
        PARTITION BY receivers.receiver_id
        ORDER BY calendar.date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),
      0
    ) AS cumulative_interaction_count
  FROM calendar
  CROSS JOIN receivers
  LEFT JOIN notification_data
    ON calendar.date = notification_data.date
    AND receivers.receiver_id = notification_data.receiver_id
)
SELECT
  cumulative_interactions.date,
  cumulative_interactions.notification_sender_profile_id,
  cumulative_interactions.receiver_id,
  cumulative_interactions.cumulative_interaction_count,
  public_profile.profile_id,
  public_profile.owned_by,
  public_profile.handle,
  public_profile.profile_picture_s3_url
FROM cumulative_interactions
JOIN `lens-public-data.polygon.public_profile` AS public_profile
  ON cumulative_interactions.receiver_id = public_profile.profile_id
  OR cumulative_interactions.receiver_id = public_profile.owned_by
ORDER BY cumulative_interactions.date DESC")

  

# Download data
data = dbGetQuery(bquery_con, sql_query)

# disconnect
dbDisconnect(bquery_con)

# remove engagement by user themselves
data = filter(data, notification_sender_profile_id != receiver_id, owned_by != receiver_id)

# calculate rankings
data_formatted <- data %>%
  filter(cumulative_interaction_count > 5) %>%
  arrange(date, desc(cumulative_interaction_count), handle) %>% # Order by date, count and also handle
  group_by(date) %>%
  mutate(rank = dense_rank(desc(cumulative_interaction_count) + row_number() / 1000),
         Value_rel = cumulative_interaction_count / cumulative_interaction_count[rank == 1],
         Value_lbl = cumulative_interaction_count) %>%
  group_by(handle) %>%
  filter(rank <= 10) %>%
  ungroup()

staticplot <- ggplot(data_formatted, aes(rank, group = handle, 
                                         fill = as.factor(handle), color = as.factor(handle))) +
  geom_tile(aes(y = cumulative_interaction_count/2,
                height = cumulative_interaction_count,
                width = 0.9), alpha = 0.8, color = NA) +
  geom_text(aes(y = 0, label = paste(handle, " ")), vjust = 0.2, hjust = 1) +
  geom_text(aes(y = round(cumulative_interaction_count), 
                label = round(Value_lbl), 
                hjust = 0),
            size = 3.5, fontface = "bold") +
  coord_flip(clip = "off", expand = FALSE) +
  scale_x_reverse() +
  guides(color = FALSE, fill = FALSE) +
  theme(axis.line=element_blank(),
        axis.text.x=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks=element_blank(),
        axis.title.x=element_blank(),
        axis.title.y=element_blank(),
        legend.position="none",
        panel.background=element_blank(),
        panel.border=element_blank(),
        panel.grid.major=element_blank(),
        panel.grid.minor=element_blank(),
        panel.grid.major.x = element_line(size=.1, color="grey"),
        panel.grid.minor.x = element_line(size=.1, color="grey"),
        plot.title=element_text(size=20, hjust=0.5, face="bold"),
        plot.subtitle=element_text(size=18, hjust=0.5, face="italic"),
        plot.caption =element_text(size=8, hjust=0.5, face="italic"),
        plot.background=element_blank(),
        plot.margin = margin(2, 2, 2, 4, "cm")) +
  labs(title = paste0("Top Profiles ",current_handle," Engages With"),
       subtitle = "Through: {closest_state}",
       caption = "Made by rickydata.lens") +
  theme(plot.caption = element_text(size = 16, hjust = 1, vjust = 1, face = "bold"))

anim = staticplot + transition_states(date, transition_length = 9000, state_length = 50) +
  view_follow(fixed_x = TRUE) +
  labs(title = paste0("Top Profiles ",current_handle," Engages With"),
       caption = "Made by rickydata.lens")

# For GIF
animate(anim, length(unique(data$date)), fps = 20, width = 800, height = 670,
        duration = 90, end_pause = 0, res = 100, # can lower resolution to upload image if needed
        renderer = gifski_renderer(paste0(current_handle,"_interactions.gif")))
}


# Run script for a user:
# run_for_user_interactions('stani.lens')
# run_for_user_interactions('chaoticmonk.lens')
# run_for_user_interactions('punkess.lens')
# run_for_user('mazemari.lens')
run_for_user('carlosbeltran.lens')
# run_for_user('larryscruff.lens')
# run_for_user('kipto.lens')
# run_for_user('jovana_kvrzic.lens')
# run_for_user('nader.lens')
# run_for_user('gotenks.lens')
# run_for_user('0xzelda.lens')
# run_for_user('bennyj504.lens')
         
