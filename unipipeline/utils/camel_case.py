def camel_case(snake_cased: str) -> str:
    return ''.join(word.title() for word in snake_cased.split('_'))
