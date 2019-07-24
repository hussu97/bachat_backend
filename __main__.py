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


logging.basicConfig(filename=cfg.logger['filename'],
                    filemode='a',
                    format=cfg.logger['format'],
                    datefmt=cfg.logger['datefmt'],
                    level=cfg.logger['level'])


def App():
    logging.info('Scraping started')
    SQLConnector().create_tables()
    p = multiprocessing.Pool(_getThreads())
    logging.info('Number of threads in pool - {}'.format(_getThreads()))
    p.map(processing, rewards_list)
    p.close()
    p.join()
    SQLConnector().delete_from_rewards_and_locations()
    logging.info('Scraping over')


def processing(rewards_class):
    try:
        rewards_program = rewards_class()
        results = rewards_program.results
        if len(results) != 0:
            sql_conn = SQLConnector()
            sqlDeletions(sql_conn, results[0].slug)
            sqlInsertions(sql_conn, results)
            logging.info('Successfully updated {}'.format(rewards_program.__class__.__name__))
        else :
            logging.warning('List for {} came back 0'.format(rewards_program.__class__.__name__))
    except Exception as e:
        logging.error('===============================SCRIPT WAS BROKEN BECAUSE OF {} IN {}======================'.format(e,rewards_program.__class__.__name__))


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
    expiry_date,
    link,
    contact,
    rating,
    cuisine,
    working_hours,
    website,
    slug,
    id
    ) values (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
                i.expiryDate,
                i.link,
                i.contact,
                i.rating,
                i.cuisine,
                i.workingHours,
                i.website,
                i.slug,
                i.id
            )
        )


if __name__ == '__main__':
    App()
