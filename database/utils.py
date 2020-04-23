from database import db

def Commit(*instances, **kwargs):
    # print(f'Instances {instances}')
    session = db.Session
    for instance in instances:
        session.add(instance)
    to_add = kwargs.get('to_add', [])
    for i in to_add:
        session.add(i)
    to_delete = kwargs.get('to_delete', [])
    for i in to_delete:
        i and session.delete(i)

    session.commit()
