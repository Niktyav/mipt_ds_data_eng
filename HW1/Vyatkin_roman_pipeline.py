import luigi
import pandas as pd
import io
import wget
import tarfile
import os
import gzip


#Скачаем датасет
class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter('GSE68849')
    dataset_url = luigi.Parameter("https://www.ncbi.nlm.nih.gov/geo/download/?acc=")
    out_dir = luigi.Parameter("C:\\mipt\\data_eng\\HW1\\data")
    def run(self):
        url = self.dataset_url+self.dataset_name+'&format=file'
        wget.download(url,out = self.output().path) # self.out_dir
  
    def output(self): 
        if not os.path.isdir(self.out_dir):
            os.mkdir(self.out_dir)
        out_path = self.out_dir+ '\\' + self.dataset_name +'_RAW.tar'
        return luigi.LocalTarget(out_path)

#распакуем датасет
class UnTarDataFile(luigi.Task):
    dataset_name = luigi.Parameter('GSE68849')
    dataset_url = luigi.Parameter("https://www.ncbi.nlm.nih.gov/geo/download/?acc=")
    out_dir = luigi.Parameter("C:\\mipt\\data_eng\\HW1\\data")
 
    def requires(self):   
        return DownloadDataset(self.dataset_name,self.dataset_url,self.out_dir)
    
    def run(self):
        #трюк для получения списка файлов в архиве, если архива нет требуем сам архив
        tar = tarfile.open(self.input().path, "r:")        
        tar_names = tar.getnames()
        for tar_name in tar_names:
            dir_name = tar_name[:tar_name.index('.')]
            tar.extract(tar_name,self.out_dir + '\\' + dir_name)
        tar.close()
  
    def output(self): 
        tar_names =[self.input().path]
        if os.path.exists(self.input().path):
            tar = tarfile.open(self.input().path, "r:")        
            tar_names = tar.getnames()
            tar.close()                     
        return [luigi.LocalTarget(self.out_dir + '\\' + tn[:tn.index('.')]+'\\'+ tn) for tn in tar_names]   
#распакуем файлы gz и достанем сырые данные
class UnGzDataFile(luigi.Task):
    dataset_name = luigi.Parameter('GSE68849')
    dataset_url = luigi.Parameter("https://www.ncbi.nlm.nih.gov/geo/download/?acc=")
    out_dir = luigi.Parameter("C:\\mipt\\data_eng\\HW1\\data")
    
    def requires(self):     
        return UnTarDataFile(self.dataset_name,self.dataset_url,self.out_dir)

    def run(self):
        for gzfile in self.input():
            dir_path = os.path.dirname(os.path.abspath(gzfile.path))
            op = open(dir_path+'\\'+"output_file.txt","w")  
            with gzip.open(gzfile.path, 'rb') as f:
                op.write(f.read().decode("utf-8"))
                op.close()
  
    def output(self): 
        return [luigi.LocalTarget(os.path.dirname(os.path.abspath(gzfile.path)) +'\\'+"output_file.txt") for gzfile in self.input()]   
# обработаем данные и пре5вратим их в целевые файлы
class SplitDataFile(luigi.Task):
    dataset_name = luigi.Parameter('GSE68849')
    dataset_url = luigi.Parameter("https://www.ncbi.nlm.nih.gov/geo/download/?acc=")
    out_dir = luigi.Parameter("C:\\mipt\\data_eng\\HW1\\data")
    
    def requires(self):     
        return UnGzDataFile(self.dataset_name,self.dataset_url,self.out_dir)

    def run(self):
        for dsfile in self.input():
            dir_path = os.path.dirname(os.path.abspath(dsfile.path))
            dfs = {}
            with open(dsfile.path) as f:
                write_key = None
                fio = io.StringIO()
                for l in f.readlines():
                    if l.startswith('['):
                        if write_key:
                            fio.seek(0)
                            header = None if write_key == 'Heading' else 'infer'
                            dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                        fio = io.StringIO()
                        write_key = l.strip('[]\n')
                        continue
                    if write_key:
                        fio.write(l)
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')
                for i in dfs:
                    dfs[i].to_csv(dir_path+'\\'+i+'.tsv',sep='\t')
                #удалим колонки, т.к. мы имеем в памяти требуемый датасет и это будет наименнее ресурсозатратно
                trimmed_df = dfs['Probes'].drop(columns = ['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'])
                trimmed_df.to_csv(dir_path+'\\'+'Probes_trimmed.tsv',sep='\t')
  
    def output(self): 
        return [luigi.LocalTarget(os.path.dirname(os.path.abspath(dsfile.path)) +'//'+'Probes_trimmed.tsv') for dsfile in self.input()]   

#очистим все скачанное и оставим только целевые файл  
#данный класс является запускаемым для всего оркестратора
class Cleanup(luigi.Task):
    dataset_name = luigi.Parameter('GSE68849')
    dataset_url = luigi.Parameter("https://www.ncbi.nlm.nih.gov/geo/download/?acc=")
    out_dir = luigi.Parameter("C:\\mipt\\data_eng\\HW1\\data")

    def requires(self):        
       return SplitDataFile(self.dataset_name,self.dataset_url,self.out_dir)
    def run(self):
        for root, dirs, files in os.walk(self.out_dir):
            for file in files:
                if not file.endswith(".tsv"):
                    os.remove(os.path.join(root, file))
    
    
    
if __name__ == '__main__':
    luigi.run()

