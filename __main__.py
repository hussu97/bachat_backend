import multiprocessing
import sys
import os
import logging

from db.sqlite import SQLConnector
import scraping_scripts.aus_discount_program as Aus
import scraping_scripts.zomato_helper as Zomato
import scraping_scripts.u_by_emaar as Emaar
import scraping_scripts.entertainer_helper as Entertainer
import scraping_scripts.etisalat_smiles as Smiles
import config_dev as cfg

rewards_list = [
    Entertainer.Dubai,
    Entertainer.AbuDhabi,
    Entertainer.AbuDhabiBody,
    Smiles.EtisalatSmiles,
    Zomato.Sharjah,
    Zomato.Dubai,
    Zomato.Abudhabi,
    Zomato.UmmAlQuwain,
    Zomato.Ajman,
    Zomato.AlAin,
    Zomato.RasAlKhaimah,
    Zomato.Fujairah,
    Aus.AusDiscountProgram,
    Emaar.UByEmaar
]

rewards_list = [Smiles.EtisalatSmiles]

logging.basicConfig(filename=cfg.logger['filename'],
                    filemode='a',
                    format=cfg.logger['format'],
                    datefmt=cfg.logger['datefmt'],
                    level=cfg.logger['level'])


def App():
    logging.info('Scraping started')
    p = multiprocessing.Pool(_getThreads())
    logging.info('Number of threads in pool - {}'.format(_getThreads()))
    p.map(processing, rewards_list)
    p.close()
    p.join()
    logging.info('Scraping over')


def processing(rewards_class):
    try:
        results = rewards_class().results
        sql_conn = SQLConnector()
        sqlDeletions(sql_conn, results[0].slug)
        sqlInsertions(sql_conn, results)
        logging.info('Successfully updated {}'.format(results[0].slug))
    except Exception as e:
        logging.info(e)


def _getThreads():
    """ Returns the number of available threads on a posix/win based system """
    if sys.platform == 'win32':
        return (int)(os.environ['NUMBER_OF_PROCESSORS'])
    else:
        return (int)(os.popen('grep -c cores /proc/cpuinfo').read())


def sqlDeletions(sql_conn, slug):
    if len(sql_conn.select_by_slug(slug)) > 0:
        sql_conn.delete_by_slug(slug)


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
    working_hours,
    address,
    website,
    slug
    ) values (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
                i.workingHours,
                i.address,
                i.website,
                i.slug
            )
        )


if __name__ == '__main__':
    App()
