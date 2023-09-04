import t_spinup


class Agency:

    def __init__(self, id_num, name):

        self.id = id_num
        self.name = name
        self.providers = {}

    def flatten(self):

        outdict = {}

        for attr in self.__dict__.keys():
            if attr == 'providers':
                outdict['providers'] = []
                for provider_key in self.providers.keys():
                    outdict['providers'].append(self.providers[provider_key].flatten())
            else:
                outdict[attr] = getattr(self, attr)

        return outdict


class Provider:

    def __init__(self, id_num, name, url):

        self.id = id_num
        self.name = name
        self.url = url
        self.collections = {}

    def flatten(self):

        outdict = {}

        for attr in self.__dict__.keys():
            if attr == 'collections':
                outdict['collections'] = []
                for collection_key in self.collections.keys():
                    outdict['collections'].append(self.collections[collection_key].flatten())
            else:
                outdict[attr] = getattr(self, attr)

        return outdict


class Collection:

    def __init__(self, id_num, name, url):

        self.id = id_num
        self.name = name
        self.url = url
        self.tags = []

    def flatten(self):

        return self.__dict__
