

def get_chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def remove_duplications_in_list_of_obj(chunk):
    return {tuple(employee.items()) for employee in chunk}