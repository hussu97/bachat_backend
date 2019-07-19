from db.sqlite import SQLConnector

import scraping_scripts.aus_discount_program as Aus
import scraping_scripts.zomato_gold as Zomato
import scraping_scripts.u_by_emaar as Emaar
# import scraping_scripts.entertainer as Entertainer
import scraping_scripts.etisalat_smiles as Smiles

rewards_list = [Smiles.EtisalatSmiles, Zomato.ZomatoGold,
                Aus.AusDiscountProgram, Emaar.UByEmaar]


def App():
    sql_conn = SQLConnector()
    for i in rewards_list:
        results = i().results
        sqlDeletions(sql_conn, results[0].rewardOrigin)
        sqlInsertions(sql_conn, results)
        print('Successfully updated {}'.format(results[0].rewardOrigin))


def sqlDeletions(sql_conn, rewardOrigin):
    if len(sql_conn.select_by_reward_origin(rewardOrigin)) > 0:
        sql_conn.delete_by_reward_origin(rewardOrigin)


def sqlInsertions(sql_conn, results):
    INSERT_STATEMENT = """INSERT INTO rewards(
    reward_origin, 
    reward_origin_logo, 
    background_image,
    logo,
    offer, 
    offer_description,
    offer_type, 
    company_name,
    cost,
    terms_and_conditions,
    location,
    expiry_date,
    link,
    contact,
    rating,
    cuisine,
    working_hours
    ) values (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
    )"""
    for i in results:
        sql_conn.insert_reward(
            INSERT_STATEMENT,
            (
                i.rewardOrigin,
                i.rewardOriginLogo,
                i.backgroundImage,
                i.logo,
                i.offer,
                i.offerDescription,
                i.offerType,
                i.companyName,
                i.cost,
                i.termsAndConditions,
                i.location,
                i.expiryDate,
                i.link,
                i.contact,
                i.rating,
                i.cuisine,
                i.workingHours
            )
        )


if __name__ == '__main__':
    App()
