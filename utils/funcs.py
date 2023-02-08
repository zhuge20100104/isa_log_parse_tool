from .beans import Detail

def parse_key_val_pair(val, sign_value_map):
        val_map = sign_value_map.value_map
        sign_name = sign_value_map.sign_name
        exceed = sign_value_map.exceed
        if val in val_map:
            return Detail(sign_name, val, val_map[val])
        else:
            if exceed is not None: 
                exceed_val = exceed.exceed
                value = exceed.value
                if val >= exceed_val:
                    return Detail(sign_name, val, value)
                else:
                    return Detail(sign_name, val, "Parse_Unknown")
            else:
                return Detail(sign_name, val, "Parse_Unknown")


class Printer(object): 
    @staticmethod
    def print_delimeter(title):
        print()
        print()
        print("=========================================================")
        print("=========================================================")
        print(title)