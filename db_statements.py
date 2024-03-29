
rewards_data = 'rewards.id as id,reward_origin,reward_origin_logo,background_image,logo,offer,offer_description,offer_type,company_name,cost,terms_and_conditions,expiry_date,link,contact,rating,cuisine,working_hours,website'
programs_data = 'description, reward_origin_link, app_store_link, play_store_link'

GET_ALL_REWARDS = f"SELECT {rewards_data} FROM rewards ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_FILTERED = f"SELECT {rewards_data} FROM rewards WHERE reward_origin IN ("+"{}) ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_WITH_LOCATIONS = f"SELECT {rewards_data} FROM rewards,rewards_and_locations WHERE id=reward_id ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_WITH_LOCATIONS_FILTERED = f"SELECT {rewards_data} FROM rewards,rewards_and_locations WHERE id=reward_id AND reward_origin IN ("+"{}) ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_PROGRAM = f"SELECT {rewards_data} FROM rewards WHERE reward_origin = '"+"{}' ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_COMPANY_NAME = f"SELECT {rewards_data} FROM rewards WHERE LOWER(company_name) LIKE LOWER('"+"{}%') ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_COMPANY_NAME_FILTERED = f"SELECT {rewards_data} FROM rewards WHERE LOWER(company_name) LIKE LOWER('"+"{}%') AND reward_origin IN ({}) ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_CATEGORY = f"SELECT {rewards_data} FROM rewards WHERE offer_type LIKE '%"+"{}%' ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_CATEGORY_FILTERED = f"SELECT {rewards_data} FROM rewards WHERE offer_type LIKE '%"+"{}%' AND reward_origin IN ({}) ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_LOCATION = f"SELECT {rewards_data} FROM locations,rewards_and_locations,rewards WHERE locations.id = location_id AND rewards.id = reward_id AND lat::numeric ="+"{} AND lon::numeric ={} ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_LOCATION_FILTERED = f"SELECT {rewards_data} FROM locations,rewards_and_locations,rewards WHERE locations.id = location_id AND rewards.id = reward_id AND lat::numeric ="+"{} AND lon::numeric ={} AND reward_origin IN ({}) ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_LOCATION_ID = f"SELECT {rewards_data} FROM rewards_and_locations, locations, rewards WHERE rewards.id = reward_id AND locations.id = location_id AND location_id = '"+"{}' ORDER BY LOWER(company_name)"
GET_ALL_REWARDS_BY_LOCATION_ID_FILTERED = f"SELECT {rewards_data} FROM rewards_and_locations, locations, rewards WHERE rewards.id = reward_id AND locations.id = location_id AND location_id = '"+"{}' AND reward_origin IN ({}) ORDER BY LOWER(company_name)"

COUNT_REWARDS = "SELECT COUNT(*) FROM rewards;"
COUNT_REWARDS_FILTERED = "SELECT COUNT(*) FROM rewards WHERE reward_origin IN ({});"
COUNT_REWARDS_WITH_LOCATIONS =  "SELECT COUNT(*) FROM rewards,rewards_and_locations WHERE id=reward_id"
COUNT_REWARDS_WITH_LOCATIONS_FILTERED = "SELECT COUNT(*) FROM rewards,rewards_and_locations WHERE id=reward_id AND reward_origin IN ({})"
COUNT_REWARDS_BY_PROGRAM = "SELECT COUNT(*) FROM rewards WHERE reward_origin = '{}'"
COUNT_REWARDS_BY_COMPANY_NAME = "SELECT COUNT(*) FROM rewards WHERE LOWER(company_name) LIKE LOWER('{}%')"
COUNT_REWARDS_BY_COMPANY_NAME_FILTERED = "SELECT COUNT(*) FROM rewards WHERE LOWER(company_name) LIKE LOWER('{}%') AND reward_origin IN ({})"
COUNT_REWARDS_BY_CATEGORY = "SELECT COUNT(*) FROM rewards WHERE offer_type = '{}'"
COUNT_REWARDS_BY_CATEGORY_FILTERED = "SELECT COUNT(*) FROM rewards WHERE offer_type = '{}' AND reward_origin IN ({})"
COUNT_REWARDS_BY_LOCATION = "SELECT COUNT(*) FROM locations,rewards_and_locations,rewards WHERE locations.id = location_id AND rewards.id = reward_id AND lat::numeric ={} AND lon::numeric ={}"
COUNT_REWARDS_BY_LOCATION_FILTERED = "SELECT COUNT(*) FROM locations,rewards_and_locations,rewards WHERE locations.id = location_id AND rewards.id = reward_id AND lat::numeric ={} AND lon::numeric ={} AND reward_origin IN ({})"

GET_ALL_PROGRAMS = f"SELECT COUNT(reward_origin) AS count, reward_origin,reward_origin_logo,{programs_data} FROM rewards left join reward_origins on reward_origin=name GROUP BY reward_origin,reward_origin_logo,{programs_data} ORDER BY reward_origin"
GET_ALL_PROGRAMS_FILTERED = f"SELECT COUNT(reward_origin) AS count, reward_origin,reward_origin_logo,{programs_data} FROM rewards left join reward_origins on reward_origin=name WHERE reward_origin in ("+"{})"+f" GROUP BY reward_origin,reward_origin_logo,{programs_data} ORDER BY reward_origin"
GET_ALL_COMPANIES = "SELECT DISTINCT company_name from rewards ORDER BY company_name"
GET_ALL_COMPANIES_FILTERED = "SELECT DISTINCT company_name from rewards WHERE reward_origin IN ({}) ORDER BY company_name"
GET_ALL_CATEGORIES = "SELECT COUNT(offer_type) AS count, offer_type FROM rewards GROUP BY offer_type ORDER BY offer_type"
GET_ALL_CATEGORIES_FILTERED = "SELECT COUNT(offer_type) AS count, offer_type FROM rewards WHERE reward_origin IN ({}) GROUP BY offer_type ORDER BY offer_type"
GET_ALL_CITIES = "SELECT COUNT(*) as count, city FROM rewards_and_locations,rewards, locations WHERE rewards.id=reward_id AND locations.id=location_id GROUP BY city ORDER BY city"
GET_ALL_CITIES_FILTERED = "SELECT COUNT(*) as count, city FROM rewards_and_locations,rewards, locations WHERE locations.id=location_id AND rewards.id=reward_id AND reward_origin in ({}) GROUP BY city ORDER BY city"

GET_ALL_REWARD_LOCATIONS = "SELECT formatted_address,city,id,lat,lon FROM locations,rewards_and_locations WHERE locations.id=rewards_and_locations.location_id AND reward_id='{}'"
GET_ALL_REWARDS_BY_CITY = f"SELECT DISTINCT {rewards_data} FROM locations,rewards_and_locations,rewards WHERE rewards.id=reward_id AND locations.id=location_id AND city ="+ "'{}'"
GET_ALL_REWARDS_BY_CITY_FILTERED = f"SELECT DISTINCT {rewards_data} FROM locations,rewards_and_locations,rewards WHERE rewards.id=reward_id AND locations.id=location_id AND city = '"+"{}' AND reward_origin IN ({})"
COUNT_REWARDS_BY_CITY = "SELECT COUNT(*) FROM locations,rewards_and_locations,rewards WHERE rewards.id=reward_id AND locations.id=location_id AND city='{}'"
COUNT_REWARDS_BY_CITY_FILTERED = "SELECT COUNT(*) FROM rewards_and_locations, locations,rewards WHERE locations.id=location_id AND rewards.id=reward_id AND city='{}' AND reward_origin IN ({})"

COUNT_REWARDS_BY_LOCATION_REGION = "SELECT count(offer) as count,formatted_address, lat,lon FROM locations,rewards_and_locations,rewards WHERE locations.id = location_id AND rewards.id = reward_id AND lat BETWEEN  {} AND {} AND lon BETWEEN  {} AND {} group by lat,lon,formatted_address"
COUNT_REWARDS_BY_LOCATION_REGION_FILTERED = "SELECT count(offer) as count,formatted_address, lat,lon FROM locations,rewards_and_locations,rewards WHERE locations.id = location_id AND rewards.id = reward_id AND lat BETWEEN  {} AND {} AND lon BETWEEN  {} AND {} AND reward_origin IN ({}) group by lat,lon,formatted_address"

GET_UNIQUE_LOCATIONS_COORDINATES = "SELECT id, lat,lon FROM locations,rewards_and_locations WHERE id=location_id GROUP BY id,lat,lon"
GET_UNIQUE_LOCATIONS_COORDINATES_FILTERED = "SELECT locations.id, lat,lon FROM locations,rewards_and_locations,rewards WHERE locations.id=location_id AND rewards.id=reward_id AND reward_origin IN ({}) GROUP BY locations.id,lat,lon"

GET_LOCATION_BY_LOCATION_ID = "SELECT formatted_address,city,id,lat,lon FROM locations WHERE id = '{}'"

GET_REWARDS_TABLE = "SELECT * FROM rewards"
GET_LOCATIONS_TABLE = "SELECT * FROM locations"
GET_REWARDS_AND_LOCATIONS_TABLE = "SELECT * FROM rewards_and_locations"
GET_REWARD_ORIGINS_TABLE = "SELECT * FROM reward_origins"
GET_TIMESTAMP = "SELECT * FROM timestamp"