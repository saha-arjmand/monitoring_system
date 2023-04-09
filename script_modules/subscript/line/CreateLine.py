

def create_title_line(title, number_of_minues=50,  space_from_up="yes", space_from_down="yes"):

    if space_from_up.lower() == 'yes' and space_from_down.lower() == 'no':
        print("\n"+"  "+("-"*(number_of_minues//2)) +
              f"  {title}  "+("-"*(number_of_minues//2)))

    elif space_from_up.lower() == 'yes' and space_from_down.lower() == 'yes':
        print("\n"+"  "+("-"*(number_of_minues//2)) +
              f"  {title}  "+("-"*(number_of_minues//2))+"\n")

    elif space_from_up.lower() == 'no' and space_from_down.lower() == 'no':
        print("  "+("-"*(number_of_minues//2)) +
              f"  {title}  "+("-"*(number_of_minues//2)))

    elif space_from_up.lower() == 'no' and space_from_down.lower() == 'yes':
        print("  "+("-"*(number_of_minues//2)) +
              f"  {title}  "+("-"*(number_of_minues//2))+"\n")
    else:
        print("  Error in Create line object")
        print("  The arguments is wrong !")


def create_simple_line(number_of_minues=50, space_from_up="yes", space_from_down="yes"):

    if space_from_up.lower() == 'yes' and space_from_down.lower() == 'no':
        print("\n"+"  "+print("-"*number_of_minues))

    elif space_from_up.lower() == 'yes' and space_from_down.lower() == 'yes':
        print("\n"+"  "+("-"*number_of_minues)+"\n")

    elif space_from_up.lower() == 'no' and space_from_down.lower() == 'no':
        print("  "+print("-"*number_of_minues))

    elif space_from_up.lower() == 'no' and space_from_down.lower() == 'yes':
        print("  "+print("-"*number_of_minues)+"\n")
    else:
        print("  Error in Create line object")
        print("  The arguments is wrong !")


def title_between_line(title, number_of_minues=50, space_from_up="yes", space_from_down="yes"):

    len_title = int(len(title))

    if space_from_up.lower() == 'yes' and space_from_down.lower() == 'no':
        print("\n"+"  "+("-"*(number_of_minues))
              + "\n" + (" "*((number_of_minues//2)-len_title//2)) +
              title+(" "*(number_of_minues//2))
              + "\n"+"  "+("-"*(number_of_minues)))

    elif space_from_up.lower() == 'yes' and space_from_down.lower() == 'yes':
        print("\n"+"  "+("-"*(number_of_minues))
              + "\n"+(" "*((number_of_minues//2)-len_title//2)) +
              title + (" "*(number_of_minues//2))
              + "\n" + "  "+("-"*(number_of_minues))+"\n")

    elif space_from_up.lower() == 'no' and space_from_down.lower() == 'no':
        print("  "+("-"*(number_of_minues))
              + "\n" + (" "*((number_of_minues//2)-len_title//2)) +
              title+(" "*(number_of_minues//2))
              + "\n"+"  "+("-"*(number_of_minues)))

    elif space_from_up.lower() == 'no' and space_from_down.lower() == 'yes':
        print("  "+("-"*(number_of_minues))
              + "\n" + (" "*((number_of_minues//2)-len_title//2)) +
              title+(" "*(number_of_minues//2))
              + "\n"+"  "+("-"*(number_of_minues))+"\n")
    else:
        print("  Error in Create line object")
        print("  The arguments is wrong !")
