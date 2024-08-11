def xcom_push(key):
    def decorator(func):
        def wrapper(*args, **kwargs):
            value = func(*args, **kwargs)
            ti = kwargs['ti']
            ti.xcom_push(key=key, value=value)
            return value
        return wrapper
    return decorator

def xcom_pull(key, task_ids=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            ti = kwargs['ti']
            xcom_value = ti.xcom_pull(key=key, task_ids=task_ids)
            return func(xcom_value, *args, **kwargs)
        return wrapper
    return decorator